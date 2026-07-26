//! Collector-authored lifecycle records for the `_meta` sidecar.
//!
//! The venue explains some gaps and none of the interesting ones. A reconnect,
//! a subscription that was sent but never acked, a socket that came up and went
//! away again: the venue says nothing about any of that, and in the recording
//! it is indistinguishable from a quiet market. These records are the
//! collector's own account of the session, and every backend writes the same
//! five of them through the constructors here so that one parser reads all five
//! venues.
//!
//! ## Why they travel the venue's own hop
//!
//! [`emit`] puts the record into the WebSocket→parser channel, exactly where
//! the venue's frames go, and each backend's `handle` files anything carrying
//! [`TAG`] under `META_STREAM` before it tries to attribute the frame to a
//! symbol. Handing the reconnect loop a writer sender instead — one hop
//! shorter, and the obvious way to write it — would cost both of these:
//!
//! * **Ownership.** `run_collection` detaches the reconnect task on its error
//!   path, so a writer sender parked in that loop would outlive it. `main`
//!   recognises a dead collection task precisely by the writer channel closing
//!   (`main.rs`, the `None` arm of `writer_rx.recv()`), and a retained sender
//!   is what stops that ever happening — the same trap `run_collection`
//!   documents for `ws_tx`.
//! * **Ordering.** Bybit's subscribe acks are venue frames bound for this same
//!   file. A record written straight to the writer would overtake whatever is
//!   still queued in front of it, and `_meta` would stop being monotonic in
//!   `local_ts` — one of the things the offline quality report checks.
//!
//! A lifecycle record cannot reach a symbol file: the tag is matched first and
//! the symbol routing below it is never entered. Each backend asserts that.

use chrono::Utc;
use serde_json::{Map, Value};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::error;

use crate::queue::{Frame, Tx};

/// Marks a record as the collector's own rather than something a venue said.
pub const TAG: &str = "_collector";

/// True for a frame the collector wrote itself.
pub fn is_record(j: &Value) -> bool {
    j.get(TAG).is_some()
}

/// `{"_collector": <event>, …fields}` — the one shape every venue writes.
///
/// `fields` is a JSON object; anything else contributes nothing, so a
/// misuse still yields a well-formed, greppable record rather than a line the
/// sidecar cannot be parsed for.
fn record(event: &str, fields: Value) -> Value {
    let mut object = Map::new();
    object.insert(TAG.to_string(), Value::from(event));
    if let Value::Object(fields) = fields {
        object.extend(fields);
    }
    Value::Object(object)
}

/// What was asked for, written before the socket is dialled.
///
/// Recorded up front because the case it is most needed for is the dial that
/// never completes. Without it a consumer has to infer what was recorded from
/// what happens to be present, which is wrong whenever a subscribed feed was
/// simply silent.
pub fn subscribe(url: &str, attempt: u64, subscriptions: Value) -> Value {
    record(
        "subscribe",
        serde_json::json!({
            "url": url,
            "attempt": attempt,
            "subscriptions": subscriptions,
        }),
    )
}

/// The socket came up. A `subscribe` with no `connected` after it is a dial
/// that failed; the pair is what separates the two.
pub fn connected(url: &str) -> Value {
    record("connected", serde_json::json!({ "url": url }))
}

/// An established socket went away with an error. `connected_for_ms` tells a
/// venue that drops us after an hour from one that drops us after a second.
///
/// Only for connections that came up: a dial that never completed is
/// [`dial_failed`], because crediting the dial as time connected is how a venue
/// refusing us outright comes to look like one that dropped us.
pub fn disconnected(error: &str, connected_for_ms: u64) -> Value {
    record(
        "disconnected",
        serde_json::json!({
            "error": error,
            "connected_for_ms": connected_for_ms,
        }),
    )
}

/// The dial never completed — DNS, TLS, a refused connection — so there was no
/// socket and nothing was connected for any length of time. `dialling_for_ms`
/// is how long it took to fail, which is what separates a refusal from a stall.
pub fn dial_failed(error: &str, dialling_for_ms: u64) -> Value {
    record(
        "dial_failed",
        serde_json::json!({
            "error": error,
            "dialling_for_ms": dialling_for_ms,
        }),
    )
}

/// The socket closed without an error, which ends the retry loop and with it
/// the collection.
pub fn stream_ended(connected_for_ms: u64) -> Value {
    record(
        "stream_ended",
        serde_json::json!({ "connected_for_ms": connected_for_ms }),
    )
}

/// How a connection ended when nothing errored.
///
/// The read loops have two such exits and they mean opposite things, so
/// `connect` reports which one it took rather than collapsing both into
/// `Ok(())`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEnd {
    /// The venue closed the socket without an error.
    Eof,
    /// The hand-off to the parser refused a frame, so the loop stopped rather
    /// than read on and discard data.
    HandOffRefused,
}

/// The record that closes out a connection, if it deserves one.
///
/// `HandOffRefused` deserves none. The record would travel the very hop that
/// just refused a market-data frame, and `queue.rs` has already raised the
/// fatal signal that makes `main` write a record naming the hop from the other
/// side — a second one claiming a clean close would contradict it.
pub fn end_of_stream(end: StreamEnd, connected_for_ms: u64) -> Option<Value> {
    match end {
        StreamEnd::Eof => Some(stream_ended(connected_for_ms)),
        StreamEnd::HandOffRefused => None,
    }
}

/// Injects a lifecycle record into the same hop the venue's frames travel on,
/// so it is timestamped, ordered and stored exactly like real data.
pub fn emit(ws_tx: &Tx<Frame>, value: Value) {
    // The caller is a reconnect loop, which has no error path of its own.
    // `send` has already raised the fatal signal, so the process is on its way
    // down either way; logging is all that is left to add here.
    if let Err(error) = ws_tx.send((Utc::now(), Utf8Bytes::from(value.to_string()))) {
        error!(?error, "couldn't record a collector lifecycle event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{self, Frame, WS_HOP};

    /// One vocabulary for five venues. Whatever reads the sidecar greps these
    /// event names and reads these keys, so a backend that spelled either
    /// differently would simply be invisible to it. Every record being built
    /// here is what stops that drifting apart.
    #[test]
    fn every_lifecycle_record_is_tagged_and_named() {
        for (value, event) in [
            (
                subscribe("wss://venue/ws", 3, serde_json::json!(["btc@trade"])),
                "subscribe",
            ),
            (connected("wss://venue/ws"), "connected"),
            (disconnected("reset", 1194), "disconnected"),
            (dial_failed("connection refused", 12), "dial_failed"),
            (stream_ended(7), "stream_ended"),
        ] {
            assert!(is_record(&value), "{value}");
            assert_eq!(value[TAG], event, "{value}");
        }
    }

    /// A hand-off that refuses a frame stops the read loop, which reaches
    /// `keep_connection` on exactly the same path as a socket the venue closed
    /// cleanly. Recording it as `stream_ended` — "the socket closed without an
    /// error" — asserts the opposite of what happened, in the one file Phase 2
    /// reads to explain the gap that follows. Nothing is written instead: the
    /// hop that just refused a market-data frame is no place to put the record
    /// saying so, and `main` writes the one naming the hop from the other side.
    #[test]
    fn a_refused_hand_off_is_not_recorded_as_a_clean_close() {
        let clean = end_of_stream(StreamEnd::Eof, 1194).expect("a clean close is worth recording");
        assert_eq!(clean[TAG], "stream_ended");
        assert_eq!(clean["connected_for_ms"], 1194);

        assert!(
            end_of_stream(StreamEnd::HandOffRefused, 1194).is_none(),
            "a refused hand-off must not be filed as a clean end of stream"
        );
    }

    /// `connected_for_ms` is documented as telling a venue that drops us after
    /// an hour from one that refuses us outright, and a dial that never
    /// completed can answer neither: there was no connection to measure. The
    /// clock the caller has runs from before the dial, so reporting it as
    /// `disconnected` credits a TLS or DNS stall as time spent connected.
    #[test]
    fn a_dial_that_never_completed_is_not_a_disconnect() {
        let failed = dial_failed("dns error: failed to lookup address", 15_000);
        assert!(is_record(&failed), "{failed}");
        assert_eq!(failed[TAG], "dial_failed");
        assert_eq!(failed["dialling_for_ms"], 15_000);
        assert!(
            failed.get("connected_for_ms").is_none(),
            "nothing was connected, so there is no time connected to report: {failed}"
        );
    }

    /// The fields are the whole point: a gap in a symbol file is explained by
    /// what was asked for, when the socket came up, and why it went away.
    #[test]
    fn a_record_carries_what_a_gap_is_explained_with() {
        let requested = subscribe("wss://venue/ws", 3, serde_json::json!(["btc@trade"]));
        assert_eq!(requested["url"], "wss://venue/ws");
        assert_eq!(requested["attempt"], 3);
        assert_eq!(requested["subscriptions"], serde_json::json!(["btc@trade"]));

        assert_eq!(connected("wss://venue/ws")["url"], "wss://venue/ws");

        let gone = disconnected("Connection reset without closing handshake", 1194);
        assert_eq!(gone["error"], "Connection reset without closing handshake");
        assert_eq!(gone["connected_for_ms"], 1194);

        assert_eq!(stream_ended(7)["connected_for_ms"], 7);
    }

    /// The tag is what keeps these records out of the symbol files, so nothing
    /// the venues actually send may be mistaken for one.
    #[test]
    fn venue_frames_are_not_mistaken_for_ours() {
        for frame in [
            r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{}}"#,
            r#"{"success":true,"ret_msg":"subscribe"}"#,
            r#"{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT"}}"#,
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#,
        ] {
            let j: serde_json::Value = serde_json::from_str(frame).unwrap();
            assert!(!is_record(&j), "{frame}");
        }
    }

    /// A record reaches the parser as a frame like any other, and survives the
    /// trip as valid JSON — that is what the backends' routing then depends on.
    #[test]
    fn an_emitted_record_arrives_as_a_parseable_frame() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Frame>(WS_HOP, 2);

        emit(&tx, connected("wss://venue/ws"));

        let (_, data) = rx.try_recv().expect("the record must be handed over");
        let j: serde_json::Value = serde_json::from_str(data.as_str()).unwrap();
        assert!(is_record(&j), "{j}");
        assert_eq!(j[TAG], "connected");
    }
}
