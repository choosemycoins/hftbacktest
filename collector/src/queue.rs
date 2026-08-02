//! Bounded hand-offs between the collector's tasks, and the policy that makes
//! bounding them safe.
//!
//! Data crosses two channels on its way to disk: WebSocket reader → parser, and
//! parser → writer. Both were unbounded, which turns a stalled consumer into
//! unbounded memory growth — and the process looks healthy the whole way down.
//! It is connected, it is receiving, nothing returns an error, and then the OOM
//! killer removes it, taking every unfinished gzip member of the day with it.
//!
//! Bounding them is only half the fix, and on its own it is the dangerous half.
//! The producers are read loops with nowhere to put a rejected message, and the
//! `let _ = tx.send(..)` they were written with is harmless on an unbounded
//! channel and **silent data loss** on a bounded one. So the bound comes with a
//! policy:
//!
//! **A hand-off that cannot take a message is fatal.** Not retried, not awaited,
//! never dropped. [`Tx::send`] returns an error to callers that have an error
//! path, and independently raises a signal on the fatal channel that `main`
//! selects on — the only route open to the detached REST snapshot tasks, which
//! have no caller to return to. `main` then leaves its loop, stops the
//! producers and drains both hands-off to completion (`wind_down`), drops the
//! `Writer` so every gzip member is finished, and exits non-zero. The signal
//! names which of the two failures happened, because the sidecar record is
//! named after it and the two point a later investigation in opposite
//! directions.
//!
//! The promise reaches all the way through the shutdown, and that is the one
//! place it is easy to lose: the drain **closes** each hand-off before it reads
//! it, so a producer still running when the loop broke has its record refused
//! rather than accepted into a channel that is about to be destroyed. Draining
//! only until the queue happened to be empty — the obvious way to write it —
//! discarded exactly those records, silently, on the ordinary `systemctl stop`
//! path.
//!
//! Awaiting the send instead — real backpressure, propagated through the socket
//! to the venue — was rejected. A market-data recording that has fallen behind
//! is not repaired by slowing down, and a growing backlog is precisely the
//! failure that has to stay visible rather than be absorbed: it would also keep
//! feeding the stall watchdog evidence of life while nothing current is being
//! recorded.
//!
//! The two hops no longer share a capacity, and each is now sized from its own
//! overflow: the writer blocked in the kernel for longer than its depth on
//! 2026-07-26, and the parser was out-run by the tape on 2026-07-29. Different
//! faults, different bursts, different arithmetic — see [`WS_QUEUE_CAPACITY`]
//! and [`WRITER_QUEUE_CAPACITY`], where the measurement behind each is written
//! out. Neither number may be raised without adding one to [`burst`].
//!
//! ## Known limit: a writer wedged in a syscall
//!
//! `Writer::write` is blocking I/O called straight from `main`'s select loop. If
//! it blocks for ever — a hung mount, a device that stops answering — `main`
//! never returns to the select and so never observes the fatal signal. The bound
//! still holds (producers stop, memory does not grow) and the reason is logged
//! where it is raised, but the process stops recording without exiting. Getting
//! out of that state needs something outside this process: systemd's
//! `WatchdogSec`, or a supervisor timeout. It cannot be solved from inside the
//! loop that is stuck.

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

/// Capacity of the socket reader → parser hand-off, **in messages**.
///
/// # Sized from a measured overflow, not from an argument about the consumer
///
/// This was 4096, and the reasoning for it was about what could stall `pump`'s
/// loop: a `serde_json` parse and a non-blocking `try_send`, no `.await` and no
/// syscall, so no I/O to block on and nothing available to it but losing the
/// CPU — a scheduler-scale event of tens of milliseconds, which 4096 covered by
/// an order of magnitude. It was left there while the writer hop was raised
/// eightfold, on the grounds that the 2026-07-26 burst had drained this hop
/// throughout the event that broke the other one.
///
/// **2026-07-29 at 03:30:02 UTC** is where that ran out (see
/// [`burst::OVERFLOWED_WS_CAPACITY`]). Both `binancefuturesum` instances filled
/// this hop within 100 ms of each other — separate processes, separate sockets
/// — and exited on `full => fatal` with the `cpu` gauge showing ~85% idle and
/// 1.4% steal either side. Neither had lost the CPU, and neither had lost it at
/// the same instant as the other by coincidence. The premise was too narrow: a
/// parser has a throughput as well as a schedule, and an instantaneous tape peak
/// above what two `serde_json` parsers on 2 vCPUs get through fills this hop at
/// the difference between the two rates, for as long as the excursion lasts.
/// Nothing has to stall.
///
/// # The arithmetic
///
/// Budget: **absorb half a second of tape at the measured peak**
/// (`burst::PEAK_MSG_PER_S`). 16 384 ÷ 20 000 = **0.82s**, and it is the
/// smallest power of two that clears 500 ms (8192 gives 0.41s). The budget is
/// expressed as a multiple of the depth that failed rather than as a
/// measurement of the excursion, because there is no measurement of the
/// excursion: both processes died 204 ms into it, so 204 ms is a lower bound and
/// the only figure available. 2.5× the number known to be too small is the
/// smallest honest step.
///
/// Two things about that arithmetic, because it is what a later reader will
/// trust and both cut in opposite directions:
///
/// * **It divides by the arrival rate, not by the excess over parser
///   throughput.** `burst::fill_time_ms` models a consumer that has stopped
///   dead, which is not the fault being sized for — an out-run parser is still
///   draining, so the hop fills at (arrival − parser throughput) and the real
///   duration absorbed is 16 384 ÷ (peak − parser) rather than 16 384 ÷ peak.
///   0.82s is therefore a *floor*. The 204 ms that failed is computed exactly
///   the same way and is a floor for the same reason, so what the two figures
///   actually decide between them is the ratio, 4×, and the ratio is the honest
///   part. Sizing off the excess would need the parser's throughput measured,
///   and nothing has measured it.
/// * **The rate is the 2026-07-26 burst's**, reused because the 2026-07-29
///   excursion's own rate was never captured — see `burst::PEAK_MSG_PER_S`. If
///   that excursion was faster than 20 000 msg/s, and it broke a hop the
///   earlier one did not, then 16 384 buys proportionally less: at 32 000 msg/s
///   it is 512 ms and the budget is met with nothing to spare. That is the
///   thinnest part of this argument and the measurement worth taking next.
///
/// # What the depth costs, and the bound it runs into
///
/// Not RAM, but the figure depends on the venue and this constant governs all
/// of them. At 300–600 B a UM frame a full hop is **~10 MB**, against the
/// writer hop's ~20 MB of ordinary frames (see below). A Bybit instance
/// carrying 1–2 KB `orderbook.200` deltas is **~33 MB**, and a Lighter one
/// **~18 MB** (11.8 MB of ordinary frames plus the ~5.9 MB of subscribe
/// snapshots that cross this hop, unlike Binance's REST snapshots which are
/// handed straight to the writer). Both hops full on the worst of those is
/// ~98 MB for one instance, so the four that share the 2 GB recording host sit
/// under 400 MB even if every one of them is Bybit and every one of them is
/// wedged at once — and each of those states is itself terminal. Raising this
/// hop fourfold moved that ceiling by at most ~25 MB per instance and the
/// resting footprint by nothing, since tokio's bounded channels do not
/// preallocate.
///
/// What it costs is the speed of a diagnosis. A backlog **here** does not mean
/// data is waiting to be written — it means the stage that decides where data
/// goes cannot keep up, which is a different fault with a different fix, and it
/// is the one hop whose overflow genuinely races the stall watchdog: a parser
/// that has stopped dequeuing produces no writes either, so nothing pets the
/// watchdog, and whichever fires first is what the operator gets. The overflow
/// names the hop and files `queue_overflow` in the sidecar for Phase 2's gap
/// attribution; the watchdog can say no more than "nothing for five minutes".
/// At the measured background rate 16 384 reports in ~82s where 4096 reported in
/// ~20s — still comfortably first, and `main.rs`'s
/// `the_socket_hop_reports_a_starved_parser_before_the_stall_watchdog_does`
/// is where that ordering is pinned, with the margin it needs to survive a
/// market thinner than the one that was measured. That test caps this hop near
/// 30 000 messages and the budget above floors it at 10 000; 16 384 is the only
/// power of two in the window.
///
/// That ordering is against the **shipped** `--stall-timeout-min` of 5, and the
/// flag is deployable (`COLLECTOR_STALL_TIMEOUT_MIN`). Raising this hop raised
/// the floor under it: 82s to fill at the measured background rate and 164s at
/// half of it means the watchdog has to be **3 minutes or more** for the hop to
/// still report first, where 4096 left that safe anywhere down to 1. Lowering
/// the flag past 3 does not break the recording, but it inverts which of the
/// two guards speaks — the operator gets "nothing for two minutes" instead of
/// the hop that broke. Noted on the flag itself in `main.rs`.
pub const WS_QUEUE_CAPACITY: usize = 16384;

/// Capacity of the parser → writer hand-off, **in messages**.
///
/// # Sized from a measured overflow, not a guess
///
/// This was 4096, described here as "a proposal, not a measurement". The
/// measurement arrived on **2026-07-26 at 22:00:00 UTC**: a burst took the four
/// recorded Binance UM symbols from ~50 msg/s each to ~20 000 msg/s aggregate
/// (4402/s then 5514/s on btcusdt alone). This hop filled, the `full => fatal`
/// policy did exactly what it is for — `queue_overflow` to the sidecar, files
/// closed, exit 1 — and systemd restarted the process 5s later. One overflow in
/// 12 hours; a ~5s hole in the recording.
///
/// The load-bearing half of that measurement is what happened next: **the
/// restarted process sat through the same burst still accelerating** (6118/s on
/// btcusdt at 22:00:11) without filling anything. So the writer keeps up with
/// the rate. 4096 did not fail on throughput; it failed because 4096 ÷ 20 000
/// is 205 ms, and the writer stalled for longer than that once.
///
/// Which is unsurprising, because this is the only consumer in the process that
/// can block in the kernel: `Writer::write` gzips and writes synchronously from
/// `main`'s select loop, so dirty-page writeback, a gzip flush and the UTC-day
/// file rotation all land on it. Those are hundreds of milliseconds to seconds.
/// A bound below a second was never going to survive one.
///
/// # The arithmetic
///
/// Budget: absorb a **multi-second** stall at the measured peak. 32 768 ÷
/// 20 000 = **1.64s**, and it is the smallest power of two that clears 1.5s
/// (16 384 gives 0.82s). Smallest, not largest, on purpose — depth is not free,
/// see the byte budget below and note that anything sitting here is in RAM and
/// not on disk. Against the observed background rate of ~200 msg/s aggregate it
/// is 2.7 minutes, so "full" still means "the writer has stopped" rather than
/// "traffic was briefly bursty".
///
/// # Byte budget
///
/// The bound counts messages; the memory is capacity × payload, and payloads
/// vary by three orders of magnitude:
///
/// * A typical UM frame — `bookTicker`, `trade`, `depth@0ms` — is 300–600 B.
///   Full at 600 B: **~20 MB**, plus 2 MB of queue slots (64 B per [`Record`]).
/// * The outliers are book snapshots. A REST `depth?limit=1000` body fetched to
///   repair a sequence gap reaches **~50 KB**, and Bybit's `orderbook.200`
///   snapshot is tens of KB.
///
/// Capacity × largest frame — 32 768 × 50 KB = **1.6 GB** — would not survive
/// the 2 GB host, so it matters that it is unreachable for a reason that is a
/// rate rather than an assumption. Those snapshots are fetched under a 100/min
/// throttle (`throttler.rs`), so however long the hop takes to fill, at most
/// 100 of them can join it per minute. Two caps follow, and they are
/// independent only while the watchdog is armed:
///
/// * **Filling is quick when traffic is heavy.** 1.6s at the measured peak
///   admits ≤3 snapshots; 164s at the background rate admits ≤273, or 13 MB.
/// * **Filling slowly runs into the stall watchdog.** A hop only stays full
///   while nothing is dequeued, and nothing dequeued means `record_write` is
///   never called, so the process ends after `--stall-timeout-min` — 5 minutes
///   by default, hence ≤500 snapshots, or 25 MB. **`--stall-timeout-min 0`
///   removes this cap**: it is a documented, deployable value
///   (`COLLECTOR_STALL_TIMEOUT_MIN`, `deploy/instance.env.example`) that
///   disarms the watchdog with a warning, and with it set only the first cap
///   and the informal argument below remain.
///
/// The worst case is therefore around **50 MB for this hop** (the 32 268
/// remaining slots holding ordinary frames, plus 2 MB of slots) and under
/// 61 MB with the socket hop full at the same moment — that second figure grew
/// with the socket hop on 2026-07-29, from 55 MB, and is still under 3% of the
/// host. Memory is not what limits this number; the stall budget is.
///
/// Two things that figure does not cover, both accuracy rather than risk.
/// Frame sizes were measured on Binance UM and this constant governs every
/// backend: a Bybit instance carrying `orderbook.200` deltas at 1–2 KB is
/// nearer **65 MB** for the same 32 768 slots (**~98 MB** with its socket hop
/// full alongside), and its snapshots arrive over the WebSocket, so the 100/min
/// throttle — which gates only the Binance REST refetch — says nothing about
/// them. Four instances of that on the 2 GB recording host is under 400 MB with
/// every one of them wedged simultaneously, so the conclusion holds and the
/// number does not.
///
/// Lighter is now the largest of those, and also over the WebSocket: measured
/// 2026-07-28, its `subscribed/order_book` snapshot is **104 KB** (ETH) to
/// **141 KB** (BTC) and its `subscribed/trade` frame replays the last 50 trades
/// at up to **95 KB**; ordinary frames average 559–721 B. Both of the big ones
/// arrive on subscribe, so the burst is bounded by the market count rather than
/// by a rate: a full connect at `lighter::MAX_MARKETS` is 25 × ~236 KB ≈ 5.9 MB
/// on top of ~23 MB of ordinary frames, so **~29 MB** — inside the figure
/// above. What is *not* bounded by the connect is the repair snapshot after a
/// nonce break, which is why `lighter::REPAIR_COOLDOWN` bounds it by time
/// instead: uncapped, a lossy market repairing at its 522 ms round trip would
/// put ~190 MB of snapshots through this hop before `full => fatal` fired.
///
/// And the writer wedged in a syscall (see the module docs), where neither cap
/// applies because `main` never returns to observe anything — but even there
/// the 1.6 GB figure needs 5.5 hours of nothing but throttled snapshots
/// arriving, and any real feed fills the hop with ordinary frames first.
///
/// Two details that make the figure smaller than it looks. Tokio's bounded
/// channel does not preallocate: `mpsc::channel(n)` takes out `n` semaphore
/// permits and grows a list of 32-message blocks on demand, so raising the
/// capacity moves the ceiling and not the resting footprint. And blocks a burst
/// allocated are reinserted for reuse where they can be, so some of that ~2 MB
/// of slots stays resident afterwards, while the payloads themselves are
/// dropped on dequeue.
pub const WRITER_QUEUE_CAPACITY: usize = 32768;

/// The bursts the capacities above are sized from, measured on the recording
/// host: the writer hop's on **2026-07-26 at 22:00:00 UTC**, the socket hop's on
/// **2026-07-29 at 03:30:02 UTC**.
///
/// Test-only because nothing at run time reads them: they exist so the sizing
/// arguments above are arithmetic a test can fail on, rather than prose that
/// quietly stops being true. Raising a capacity means changing a number here
/// too, or explaining why the old measurement still applies.
#[cfg(test)]
pub mod burst {
    /// Aggregate peak across the four recorded Binance UM symbols, each with
    /// `@trade` + `@bookTicker` + `@depth@0ms`. Per symbol the observed peaks
    /// were 4402/s then 5514/s on btcusdt alone, and 6118/s eleven seconds
    /// later — the last of those survived by the restarted process.
    ///
    /// **One measurement, two bursts.** This is 2026-07-26's, and the socket
    /// hop's 2026-07-29 arithmetic borrows it because that event's rate was
    /// never captured: the gauges say what the CPU was doing and the journal
    /// says which hop overflowed, but nothing counted frames. The borrowing is
    /// the weakest link in [`super::WS_QUEUE_CAPACITY`] and it is deliberately
    /// left visible rather than papered over with a second constant nobody
    /// measured. An excursion faster than this shortens every socket-hop figure
    /// derived below in proportion. Counting frames per second into the sidecar
    /// would close it, and until something does, a third burst is worth more
    /// than another round of reasoning.
    pub const PEAK_MSG_PER_S: usize = 20_000;

    /// What the same four symbols run at outside a burst: ~50 msg/s each.
    pub const BACKGROUND_MSG_PER_S: usize = 200;

    /// The socket-hop depth that overflowed on **2026-07-29 at 03:30:02 UTC**,
    /// kept as the number the replacement has to beat.
    ///
    /// The second measured burst, and the first one this hop failed. Both
    /// `binancefuturesum` instances — separate processes, separate sockets,
    /// separate connections to the venue — filled `websocket->parser` within
    /// 100 ms of each other and exited on `full => fatal`, each leaving a ~6s
    /// hole and restarting clean. What makes it a measurement of the tape rather
    /// than of the host is the coincidence plus the `cpu` gauge either side of
    /// it: ~85% idle, 1.4% steal. The instances did not run out of CPU, and they
    /// did not run out of it *simultaneously by chance* — the market moved.
    ///
    /// It falsifies the reasoning this hop was sized by. The old argument was
    /// that its consumer cannot block on I/O, so the only stall available to it
    /// is losing the CPU, which is a scheduler-scale event of tens of
    /// milliseconds — and that 4096 messages was an order of magnitude over
    /// that. Both halves are still true, and they were not the failure. Two
    /// `serde_json` parsers on 2 vCPUs have a *throughput*, and an instantaneous
    /// tape peak that exceeds it does not need the parser to stop for anything:
    /// the hop fills at the difference between the two rates for as long as the
    /// excursion lasts. So this hop is now sized by the duration of a rate
    /// excursion, exactly as the writer hop is sized by the duration of a stall.
    ///
    /// The one thing the incident does not give is how long the excursion ran
    /// for. Both processes died at the moment they found out, so all that is
    /// known is that it outlasted `fill_time_ms(OVERFLOWED_WS_CAPACITY,
    /// PEAK_MSG_PER_S)` = 204 ms. A lower bound, not a measurement — which is
    /// why the budget in the test below is stated as a multiple of it.
    ///
    /// The writer hop is untouched by all this and stays at 32 768: it held
    /// through the same event with 8106 and 9064 records queued (a quarter of
    /// its depth), and both backlogs were drained to disk on the way out. That
    /// is the second time it has been measured and the first time it has been
    /// measured *not* failing.
    pub const OVERFLOWED_WS_CAPACITY: usize = 4096;

    /// How long a hop of `capacity` survives a consumer that has stopped
    /// draining, at `rate` messages per second.
    pub const fn fill_time_ms(capacity: usize, rate: usize) -> usize {
        capacity * 1000 / rate
    }
}

/// Capacity of the REST poller → writer hand-off, **in messages**.
///
/// A hop of its own rather than a share of [`WRITER_QUEUE_CAPACITY`], and the
/// reason is not throughput — it is that `main` has to be able to tell a record
/// the venue sent from one the collector produced on a timer, and the two are
/// identical once they are in a queue: same type, same stream name (the poller
/// files under `BTCUSDT`, exactly as the WebSocket frames do), same file. The
/// hop they arrive on is the distinction. `main` pets the stall watchdog from
/// the venue arm and not from this one — see `watchdog::Source`, and the
/// measurement in it for what happens when the two are conflated.
///
/// Sized for the one producer there is: `binancefuturesum` files at most one
/// element per recorded symbol per [`binancefuturesum::PREMIUM_INDEX_INTERVAL`],
/// so 1024 is more than eighty cycles of a twelve-symbol instance, or one cycle
/// of an instance carrying every symbol the venue lists (851, measured
/// 2026-07-28) with room over. At a few hundred bytes an element that is well
/// under a megabyte. It cannot fill for the reason the writer hop can: filling
/// needs the writer to have stopped, and at six records a minute per symbol the
/// writer hop reaches its own bound first by orders of magnitude. The bound is
/// here so that the `full => fatal` policy holds on every hop without exception,
/// not because this one is expected to be reached.
///
/// [`binancefuturesum::PREMIUM_INDEX_INTERVAL`]: crate::binancefuturesum::PREMIUM_INDEX_INTERVAL
pub const POLLER_QUEUE_CAPACITY: usize = 1024;

/// Hop names, used in the fatal message so an operator can tell which of the
/// three stalled without reading the code.
pub const WS_HOP: &str = "websocket->parser";
pub const WRITER_HOP: &str = "parser->writer";
pub const POLLER_HOP: &str = "poller->writer";

/// A record on its way to the writer: when it arrived, which stream it belongs
/// to, and the payload exactly as the venue sent it.
pub type Record = (DateTime<Utc>, String, String);

/// A frame on its way from the socket to the parser.
pub type Frame = (DateTime<Utc>, Utf8Bytes);

/// Sidecar event name for a hand-off that had no room left.
pub const OVERFLOW_EVENT: &str = "queue_overflow";

/// Sidecar event name for a hand-off whose receiver is gone.
///
/// Kept distinct from [`OVERFLOW_EVENT`] because the two say opposite things
/// about the same recording, and `_meta` is what the offline quality report
/// attributes gaps with. An overflow means the collector could not keep up; a
/// closed hop means the consumer had already stopped, which is the ordinary
/// consequence of a collection task returning while a detached reconnect task
/// still holds a live socket.
pub const CLOSED_EVENT: &str = "hand_off_closed";

/// Why a hand-off refused a message. Both variants are terminal.
#[derive(Debug, Error)]
pub enum SendError {
    #[error("the {hop} queue is full ({capacity} messages); the consumer is not keeping up")]
    Full { hop: &'static str, capacity: usize },
    #[error("the {hop} queue has no receiver left; nothing further can be recorded")]
    Closed { hop: &'static str },
}

impl SendError {
    /// What the sidecar should call this.
    pub fn event(&self) -> &'static str {
        match self {
            SendError::Full { .. } => OVERFLOW_EVENT,
            SendError::Closed { .. } => CLOSED_EVENT,
        }
    }
}

/// A report that recording has broken, carried to `main`.
///
/// `event` and `reason` are separate because they have separate readers: the
/// event name is what a sidecar consumer greps for, the reason is the sentence
/// an operator reads in the journal.
#[derive(Debug, Clone)]
pub struct Fatal {
    pub event: &'static str,
    pub reason: String,
}

/// The producer's end of the fatal signal. Cloned into every sender.
#[derive(Clone)]
pub struct FatalTx(mpsc::Sender<Fatal>);

impl FatalTx {
    /// Reports a condition that must stop the process.
    ///
    /// Logged as well as signalled: if the consumer of the signal is itself
    /// wedged (see the module docs) the log line is the only thing that will
    /// say what happened.
    ///
    /// The signal channel holds one message, and a failure to enqueue is
    /// deliberately ignored — it means a report is already waiting, and the
    /// first one is the one that describes the original fault. Blocking here to
    /// deliver a duplicate would deadlock the producer in exactly the situation
    /// the report exists for.
    pub fn raise(&self, fatal: Fatal) {
        error!(
            event = fatal.event,
            reason = fatal.reason,
            "a data hand-off failed; the collector will stop"
        );
        let _ = self.0.try_send(fatal);
    }
}

/// The consumer's end of the fatal signal. There is one, in `main`.
pub struct FatalRx {
    rx: mpsc::Receiver<Fatal>,
    /// Held so the channel can never report "every sender is gone". Without it
    /// `recv` would resolve immediately once the collection task died, spinning
    /// the select loop that is meant to be waiting on it.
    _keepalive: FatalTx,
}

impl FatalRx {
    /// Resolves when a producer reports that a hand-off failed.
    pub async fn recv(&mut self) -> Fatal {
        match self.rx.recv().await {
            Some(fatal) => fatal,
            // Unreachable while `_keepalive` is held. Waiting for ever is the
            // safe answer anyway: the alternative in a `select!` arm is a busy
            // loop, and a panic here would skip the writer's `Drop`.
            None => std::future::pending().await,
        }
    }
}

pub fn fatal_channel() -> (FatalTx, FatalRx) {
    // One slot: `main` acts on the first report and exits, so a second would
    // never be read.
    let (tx, rx) = mpsc::channel(1);
    let tx = FatalTx(tx);
    (tx.clone(), FatalRx { rx, _keepalive: tx })
}

/// A bounded sender that treats "no room" and "no receiver" as fatal.
pub struct Tx<T> {
    tx: mpsc::Sender<T>,
    fatal: FatalTx,
    hop: &'static str,
    capacity: usize,
}

// Derived `Clone` would demand `T: Clone`, which neither payload is.
impl<T> Clone for Tx<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            fatal: self.fatal.clone(),
            hop: self.hop,
            capacity: self.capacity,
        }
    }
}

impl<T> Tx<T> {
    /// Hands a message over, or reports that recording has broken.
    ///
    /// Never waits: see the module docs for why backpressure is the wrong
    /// answer here. The returned error exists so that callers with an error
    /// path propagate it; the fatal signal is raised regardless, because some
    /// callers are detached tasks that have no such path.
    pub fn send(&self, item: T) -> Result<(), SendError> {
        let error = match self.tx.try_send(item) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => SendError::Full {
                hop: self.hop,
                capacity: self.capacity,
            },
            Err(mpsc::error::TrySendError::Closed(_)) => SendError::Closed { hop: self.hop },
        };
        self.fatal.raise(Fatal {
            event: error.event(),
            reason: error.to_string(),
        });
        Err(error)
    }

    /// The fatal signal this hand-off reports to, so a downstream hop can be
    /// wired to the same one without threading it through every signature.
    pub fn fatal(&self) -> FatalTx {
        self.fatal.clone()
    }
}

pub fn bounded<T>(
    hop: &'static str,
    capacity: usize,
    fatal: FatalTx,
) -> (Tx<T>, mpsc::Receiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        Tx {
            tx,
            fatal,
            hop,
            capacity,
        },
        rx,
    )
}

/// A hand-off wired to a fatal channel of its own, for tests that need to fill
/// one up and then inspect what the producer did about it.
#[cfg(test)]
pub fn test_bounded<T>(hop: &'static str, capacity: usize) -> (Tx<T>, mpsc::Receiver<T>, FatalRx) {
    let (fatal_tx, fatal_rx) = fatal_channel();
    let (tx, rx) = bounded(hop, capacity, fatal_tx);
    (tx, rx, fatal_rx)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    fn record(payload: &str) -> Record {
        (Utc::now(), "BTC".to_string(), payload.to_string())
    }

    /// The sizing basis for the writer hop, and the reason the capacity is not
    /// the 4096 it shipped with.
    ///
    /// The load-bearing half of the 2026-07-26 measurement is not that the hop
    /// filled — it is that the *restarted* process then sat through the same
    /// burst still accelerating (6118/s on btcusdt at 22:00:11) without filling
    /// anything. So the writer keeps up with the rate; what it could not do was
    /// ride out one stall of its own. 4096 at the measured peak is 205 ms of
    /// tolerance, and the writer is the one consumer in this process that can
    /// block in the kernel — dirty-page writeback, a gzip flush, the day's file
    /// rotation. Those are measured in hundreds of milliseconds to seconds, so
    /// a bound below a second was never going to hold.
    #[test]
    fn the_writer_hop_rides_out_a_multi_second_stall_at_the_measured_burst() {
        // A second and a half: comfortably past the ~0.2s that broke it, past
        // the writeback and rotation stalls that are the plausible causes, and
        // still short enough that a full queue means the writer has stopped.
        const REQUIRED_MS: usize = 1_500;

        let tolerated = burst::fill_time_ms(WRITER_QUEUE_CAPACITY, burst::PEAK_MSG_PER_S);
        assert!(
            tolerated >= REQUIRED_MS,
            "the parser->writer hop tolerates only {tolerated} ms of writer stall at the \
             measured burst rate of {} msg/s ({WRITER_QUEUE_CAPACITY} messages); the \
             2026-07-26 22:00 UTC burst outlasted 205 ms of it, and the budget is \
             {REQUIRED_MS} ms",
            burst::PEAK_MSG_PER_S,
        );

        // The other side of the same budget: pinned to the *smallest* capacity
        // that meets it, because depth costs RAM-resident data and delays the
        // moment "full" starts meaning something. Doubling it needs a bigger
        // measured burst, not a bigger appetite — which means changing
        // `burst::PEAK_MSG_PER_S`, where the reason has to be written down.
        let halved = burst::fill_time_ms(WRITER_QUEUE_CAPACITY / 2, burst::PEAK_MSG_PER_S);
        assert!(
            halved < REQUIRED_MS,
            "half the capacity would already cover the {REQUIRED_MS} ms budget ({halved} ms), \
             so {WRITER_QUEUE_CAPACITY} messages is more depth than the measurement justifies"
        );
    }

    /// The sizing basis for the socket hop, and the reason the capacity is not
    /// the 4096 it shipped with.
    ///
    /// That number came from an argument about the consumer: `pump`'s loop is a
    /// `serde_json` parse and a non-blocking `try_send`, with no `.await` and no
    /// syscall in it, so the only stall available to it is losing the CPU — tens
    /// of milliseconds — and 4096 was an order of magnitude over that. On
    /// 2026-07-29 the hop overflowed on both `binancefuturesum` instances at
    /// once with the host 85% idle, which says the parser never stalled at all:
    /// it was out-run. A parser has a throughput as well as a schedule, and a
    /// tape peak above it fills the hop for as long as the excursion lasts.
    ///
    /// So the budget is a duration of excursion, and the only figure the
    /// incident yields is the one that was **not** enough.
    ///
    /// Both figures divide by the arrival rate, which is `fill_time_ms`'s model
    /// of a consumer that has stopped dead rather than one that is merely
    /// out-run — so each is a floor, and what they decide between them is the
    /// 4× ratio. The rate itself is 2026-07-26's, borrowed. Both caveats are
    /// written out at [`WS_QUEUE_CAPACITY`] and [`burst::PEAK_MSG_PER_S`];
    /// neither can be closed without a measurement this incident did not leave.
    #[test]
    fn the_socket_hop_outlasts_the_excursion_that_overflowed_it() {
        // Two and a half times the depth that demonstrably failed. Not a
        // measurement of the excursion — nothing measured it, both processes
        // died at 205 ms — but the smallest budget that is not simply a repeat
        // of the number already known to be too small.
        const REQUIRED_MS: usize = 500;

        let known_insufficient =
            burst::fill_time_ms(burst::OVERFLOWED_WS_CAPACITY, burst::PEAK_MSG_PER_S);
        assert!(
            REQUIRED_MS >= 2 * known_insufficient,
            "the budget ({REQUIRED_MS} ms) has to clear the {known_insufficient} ms that \
             overflowed on 2026-07-29 by a margin, or it is the same guess again"
        );

        let tolerated = burst::fill_time_ms(WS_QUEUE_CAPACITY, burst::PEAK_MSG_PER_S);
        assert!(
            tolerated >= REQUIRED_MS,
            "the websocket->parser hop absorbs only {tolerated} ms of tape at the measured \
             peak of {} msg/s ({WS_QUEUE_CAPACITY} messages); the 2026-07-29 03:30 UTC \
             excursion outlasted {known_insufficient} ms of it, and the budget is \
             {REQUIRED_MS} ms",
            burst::PEAK_MSG_PER_S,
        );

        // The other side of the budget, as on the writer hop: pinned to the
        // smallest capacity that meets it. Depth here is not free in a way that
        // RAM does not capture — this hop's overflow is the process's only
        // specific diagnosis of a parser that cannot keep up, and burying it
        // deeper delays that report towards the stall watchdog, which can say
        // no more than "silence". The upper bound is enforced where that race
        // is: `the_socket_hop_reports_a_starved_parser_before_the_stall_
        // watchdog_does` in `main.rs`.
        let halved = burst::fill_time_ms(WS_QUEUE_CAPACITY / 2, burst::PEAK_MSG_PER_S);
        assert!(
            halved < REQUIRED_MS,
            "half the capacity would already cover the {REQUIRED_MS} ms budget ({halved} ms), \
             so {WS_QUEUE_CAPACITY} messages is more depth than the measurement justifies"
        );
    }

    /// The whole point of the bound. A producer that cannot hand a message over
    /// must not be able to pretend it did: `send` reports the failure to its
    /// caller *and* raises the process-wide fatal signal, because the detached
    /// tasks that hold a sender have no caller to report to.
    #[tokio::test]
    async fn a_full_queue_is_fatal_not_a_dropped_message() {
        let (fatal_tx, mut fatal_rx) = fatal_channel();
        let (tx, mut rx) = bounded::<Record>(WRITER_HOP, 2, fatal_tx);

        tx.send(record("first")).unwrap();
        tx.send(record("second")).unwrap();

        let error = tx.send(record("third")).unwrap_err();
        assert!(matches!(error, SendError::Full { .. }), "{error}");

        let raised = timeout(Duration::from_secs(1), fatal_rx.recv())
            .await
            .expect("an overflow must raise the fatal signal, not only return an error");
        assert!(
            raised.reason.contains(WRITER_HOP),
            "the signal must name the hop that broke: {}",
            raised.reason
        );

        // Nothing already accepted is lost on the way to the failure, and the
        // rejected message does not turn up later pretending it was accepted.
        assert_eq!(rx.try_recv().unwrap().2, "first");
        assert_eq!(rx.try_recv().unwrap().2, "second");
        assert!(
            rx.try_recv().is_err(),
            "the rejected message must not be queued"
        );
    }

    /// A receiver that has gone means nothing further can ever be recorded, so
    /// it reaches the same path for the same reason.
    #[tokio::test]
    async fn a_closed_queue_is_fatal_too() {
        let (fatal_tx, mut fatal_rx) = fatal_channel();
        let (tx, rx) = bounded::<Record>(WS_HOP, 2, fatal_tx);
        drop(rx);

        let error = tx.send(record("nowhere to go")).unwrap_err();
        assert!(matches!(error, SendError::Closed { .. }), "{error}");

        let raised = timeout(Duration::from_secs(1), fatal_rx.recv())
            .await
            .expect("a closed hand-off must raise the fatal signal");
        assert!(raised.reason.contains(WS_HOP), "{}", raised.reason);
    }

    /// The two failures reach the same path but are not the same event, and the
    /// sidecar is where the difference is read: `_meta` is what the offline
    /// report attributes gaps with.
    ///
    /// A hop losing its receiver is the ordinary consequence of a collection
    /// task ending — Bybit's rejected subscribe returns, dropping `ws_rx`,
    /// while the detached reconnect task is still reading a live socket — and
    /// the very next frame raises `Closed`. Recording that as an overflow tells
    /// whoever reads the file afterwards that the collector could not keep up,
    /// when nothing was ever full.
    #[tokio::test]
    async fn the_signal_names_which_of_the_two_failures_happened() {
        let (fatal_tx, mut fatal_rx) = fatal_channel();
        let (tx, mut rx) = bounded::<Record>(WRITER_HOP, 1, fatal_tx);
        tx.send(record("fills it")).unwrap();
        tx.send(record("rejected")).unwrap_err();
        assert_eq!(fatal_rx.recv().await.event, OVERFLOW_EVENT);

        let (fatal_tx, mut fatal_rx) = fatal_channel();
        let (tx, rx2) = bounded::<Record>(WS_HOP, 1, fatal_tx);
        drop(rx2);
        tx.send(record("nowhere to go")).unwrap_err();
        assert_eq!(fatal_rx.recv().await.event, CLOSED_EVENT);

        // Nothing above depends on the queue being drained; keep the receiver
        // alive so the first hop reported `Full` rather than `Closed`.
        drop(rx.try_recv());
    }

    /// Reporting must never be what blocks a producer. The first report is the
    /// one that matters and the rest are noise, but a producer that stalled
    /// trying to report would deadlock in exactly the situation the report
    /// exists for.
    #[tokio::test]
    async fn reporting_repeatedly_neither_blocks_nor_panics() {
        let (fatal_tx, mut fatal_rx) = fatal_channel();
        let (tx, _rx) = bounded::<Record>(WRITER_HOP, 1, fatal_tx);

        tx.send(record("fills it")).unwrap();
        for _ in 0..1_000 {
            assert!(tx.send(record("rejected")).is_err());
        }

        assert!(
            timeout(Duration::from_secs(1), fatal_rx.recv())
                .await
                .is_ok(),
            "the first report must still be readable after the rest were shed"
        );
    }
}
