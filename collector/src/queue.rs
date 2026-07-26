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
//! have no caller to return to. `main` then leaves its loop, writes whatever is
//! still queued (`drain_backlog`), drops the `Writer` so every gzip member is
//! finished, and exits non-zero. The signal names which of the two failures
//! happened, because the sidecar record is named after it and the two point a
//! later investigation in opposite directions.
//!
//! Awaiting the send instead — real backpressure, propagated through the socket
//! to the venue — was rejected. A market-data recording that has fallen behind
//! is not repaired by slowing down, and a growing backlog is precisely the
//! failure that has to stay visible rather than be absorbed: it would also keep
//! feeding the stall watchdog evidence of life while nothing current is being
//! recorded.
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

/// Capacity of each bounded hand-off, **in messages**.
///
/// The bound counts messages, not bytes: every item owns a payload of whatever
/// length the venue sent, so the memory budget is capacity × realistic maximum
/// frame.
///
/// * A typical frame — a Bybit orderbook delta, a Binance `depthUpdate` — is
///   1–2 KB, so a full queue holds roughly 8 MB.
/// * The largest frames are full-depth snapshots: a Bybit `orderbook.200`
///   snapshot is tens of KB, and the REST `depth?limit=1000` body fetched to
///   repair a Binance gap is around 100 KB. Those are throttled to 100/min
///   (`throttler.rs`), so they cannot fill a queue by themselves.
///
/// Worst case is therefore on the order of 100 MB per hop, ~200 MB with both
/// hops full at once, against the 2 GB of the deployment host. That is enough
/// headroom that hitting the bound means "the consumer has stopped", not
/// "traffic was briefly bursty" — and small enough to leave the host usable
/// while the process is on its way down. At the volumes measured on 2026-07-25
/// (86–159 MB/day/symbol for Bybit, 22 MB/day for Hyperliquid) a full queue is
/// on the order of a minute of traffic, so a rotation or a gzip flush cannot
/// come near it.
///
/// This number is a proposal, not a measurement — open decision 3 of
/// `docs/design-multi-venue-collection.md`. It should be recomputed from an
/// observed peak message rate once one has been recorded.
pub const QUEUE_CAPACITY: usize = 4096;

/// Hop names, used in the fatal message so an operator can tell which of the
/// two stalled without reading the code.
pub const WS_HOP: &str = "websocket->parser";
pub const WRITER_HOP: &str = "parser->writer";

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
