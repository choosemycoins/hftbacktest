//! The loop every backend runs between its socket and the writer.
//!
//! It was five copies of the same twenty lines, differing only in which
//! `handle` they called. That is cheap duplication right up until one of the
//! copies has to be correct about something subtle, and this one does:
//!
//! * **The producer takes the only `ws_tx`.** `main` recognises a dead
//!   collection task by the writer channel closing, and that only happens once
//!   this function returns, which only happens once the last sender is gone.
//!   A clone retained here turns a dead producer into a process that sits idle
//!   recording nothing while systemd reports it perfectly healthy. Passing by
//!   value is what makes the exit reachable, and there is now one place to get
//!   that right instead of five.
//! * **A refused hand-off ends the task.** Bounded channels turned the old
//!   `let _ = tx.send(..)` into silent data loss, so `handle` returns the
//!   failure and this loop stops on it rather than reading the next frame.
//!
//! What stays per backend is the frame handling itself — the symbol routing,
//! Binance's sequence-gap bookkeeping, Bybit's rejected-subscribe check — which
//! is passed in.

use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use crate::{
    error::ConnectorError,
    queue::{self, Frame, Record, Tx, WS_QUEUE_CAPACITY},
};

/// Runs `producer` and files everything it delivers, returning when the
/// producer has released its sender or the recording has broken.
pub async fn pump<P, F, H>(
    writer_tx: Tx<Record>,
    producer: P,
    mut handle: H,
) -> Result<(), anyhow::Error>
where
    P: FnOnce(Tx<Frame>) -> F,
    F: Future<Output = ()> + Send + 'static,
    H: FnMut(&Tx<Record>, DateTime<Utc>, Utf8Bytes) -> Result<(), ConnectorError>,
{
    let (ws_tx, mut ws_rx) = queue::bounded(queue::WS_HOP, WS_QUEUE_CAPACITY, writer_tx.fatal());
    // By value. See the module docs, and `producer_finishing_with_a_clean_eof_
    // ends_the_collection` for what a retained clone costs.
    let h = tokio::spawn(producer(ws_tx));

    while let Some((recv_time, data)) = ws_rx.recv().await {
        match handle(&writer_tx, recv_time, data) {
            Ok(()) => {}
            // Not a bad frame but a broken recording. Returning ends the task,
            // which closes the writer channel and brings `main` down with the
            // files finished.
            Err(error) if error.is_fatal() => {
                error!(?error, "fatal stream error; stopping the collection task.");
                return Err(error.into());
            }
            Err(error) => {
                error!(?error, "couldn't handle the received data.");
            }
        }
    }
    let _ = h.await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::*;
    use crate::queue::WRITER_HOP;

    const FRAME: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;

    /// Stands in for a backend's `handle`: files everything under one symbol,
    /// and reports a refused hand-off rather than swallowing it.
    fn file_everything(
        writer_tx: &Tx<Record>,
        recv_time: DateTime<Utc>,
        data: Utf8Bytes,
    ) -> Result<(), ConnectorError> {
        writer_tx.send((recv_time, "BTC".to_string(), data.to_string()))?;
        Ok(())
    }

    /// A producer that stops is the collector's last line of defence: the main
    /// loop treats the writer channel closing as fatal and exits non-zero
    /// (`main.rs`, the `None` arm of `writer_rx.recv()`). That only fires if
    /// the collection task actually returns, and it only returns once the last
    /// `ws_tx` is gone. Keeping a clone alongside the producer's — as all five
    /// backends used to — turned a dead producer into an idle process that
    /// systemd reports as perfectly healthy while nothing is being recorded.
    ///
    /// Virtual time: the deadline is an anti-hang guard, not a performance
    /// budget, and on real time it was observed failing under nothing worse
    /// than a busy machine. Paused, the clock only advances when every task is
    /// parked, so it can only be reached by an actual deadlock.
    #[tokio::test(start_paused = true)]
    async fn producer_finishing_with_a_clean_eof_ends_the_collection() {
        let (writer_tx, mut writer_rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);

        let done = tokio::time::timeout(
            Duration::from_secs(5),
            pump(
                writer_tx,
                |ws_tx| async move {
                    ws_tx.send((Utc::now(), Utf8Bytes::from(FRAME))).unwrap();
                    // Returning drops the sender, exactly as `keep_connection`
                    // does when the socket closes without an error.
                },
                file_everything,
            ),
        )
        .await;

        assert!(
            done.is_ok(),
            "pump must return once the producer releases the last ws_tx"
        );
        done.unwrap().unwrap();

        // The frame sent before the producer exited is not lost on the way out.
        let (_, stream, _) = writer_rx.try_recv().expect("the frame must be written");
        assert_eq!(stream, "BTC");
    }

    /// The other end of the same guarantee. A writer that has stopped draining
    /// must end the collection task, because that is what makes `main` exit
    /// non-zero with the files closed. Logging and reading the next frame — the
    /// behaviour a bounded channel inherits from `let _ = send` — would instead
    /// discard market data silently for as long as the process stayed up.
    #[tokio::test(start_paused = true)]
    async fn a_writer_that_stops_draining_ends_the_collection() {
        // The receiver is held but never read, so the queue fills and stays
        // full; capacity 1 just gets there in one frame instead of
        // `WRITER_QUEUE_CAPACITY`.
        let (writer_tx, _writer_rx, mut fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            pump(
                writer_tx,
                |ws_tx| async move {
                    // The socket keeps delivering while the writer is stuck.
                    // This hop has room, so the producer notices nothing.
                    for _ in 0..4 {
                        ws_tx.send((Utc::now(), Utf8Bytes::from(FRAME))).unwrap();
                    }
                },
                file_everything,
            ),
        )
        .await
        .expect("pump must not hang once the writer stops draining");

        assert!(
            result.is_err(),
            "the collection task must fail rather than carry on dropping frames"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), fatal.recv())
                .await
                .is_ok(),
            "and the reason must reach main, which is the half a returned error cannot cover"
        );
    }

    /// The socket hop is built here and nowhere else, so this is the only place
    /// the two capacities can be confused — and the whole reason there are two.
    ///
    /// `queue.rs` argues the socket hop must stay the shallower of the two: its
    /// overflow is the one that races the stall watchdog, and it has to win that
    /// race with a diagnosis where the watchdog would only report silence.
    /// Wiring this call to `WRITER_QUEUE_CAPACITY` would make it twice as deep
    /// and undo that silently — the drain in `main` has
    /// `the_drain_is_bounded_even_if_the_producers_keep_pushing` guarding the
    /// symmetric mistake, and `queue.rs`'s own tests only check arithmetic over
    /// the constants, never which one arrives here.
    ///
    /// Measuring it rather than reading the source: the producer has no `.await`
    /// in its loop, so on the current-thread runtime it runs to the first
    /// refusal before `pump` is ever polled again. What it counts is the hop's
    /// depth and nothing else.
    #[tokio::test(start_paused = true)]
    async fn the_socket_hop_is_built_with_the_socket_hop_capacity() {
        let (writer_tx, _writer_rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();

        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            pump(
                writer_tx,
                move |ws_tx| async move {
                    while ws_tx.send((Utc::now(), Utf8Bytes::from(FRAME))).is_ok() {
                        counted.fetch_add(1, Ordering::Relaxed);
                    }
                },
                // Nothing reaches the writer hop, so its capacity of 1 cannot
                // be what the count measures.
                |_, _, _| Ok(()),
            ),
        )
        .await;

        assert_eq!(
            accepted.load(Ordering::Relaxed),
            WS_QUEUE_CAPACITY,
            "the socket hop must be built with WS_QUEUE_CAPACITY; a hop of \
             WRITER_QUEUE_CAPACITY here would bury a parser that cannot keep up \
             under twice the buffer and let the stall watchdog report it first, \
             as silence"
        );
    }

    /// An unreadable frame is not a broken recording. Ending the task on one
    /// would turn a single malformed message — or a venue field that changed
    /// shape — into a restart loop.
    #[tokio::test(start_paused = true)]
    async fn an_unparseable_frame_is_logged_and_the_loop_continues() {
        let (writer_tx, mut writer_rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);

        tokio::time::timeout(
            Duration::from_secs(5),
            pump(
                writer_tx,
                |ws_tx| async move {
                    ws_tx
                        .send((Utc::now(), Utf8Bytes::from("not json")))
                        .unwrap();
                    ws_tx.send((Utc::now(), Utf8Bytes::from(FRAME))).unwrap();
                },
                |writer_tx, recv_time, data| {
                    let _: serde_json::Value = serde_json::from_str(data.as_str())?;
                    file_everything(writer_tx, recv_time, data)
                },
            ),
        )
        .await
        .expect("pump must return once the producer releases the last ws_tx")
        .expect("a bad frame must not end the collection");

        assert_eq!(
            writer_rx.try_recv().expect("the good frame must follow").1,
            "BTC"
        );
    }
}
