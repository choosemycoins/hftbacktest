//! Collector-authored lifecycle records for the `_meta` sidecar.
//!
//! The venue explains some gaps and none of the interesting ones. A reconnect,
//! a subscription that was sent but never acked, a socket that came up and went
//! away again: the venue says nothing about any of that, and in the recording
//! it is indistinguishable from a quiet market. These records are the
//! collector's own account of the session, and every backend writes the same
//! five of them through the constructors here so that one parser reads all five
//! venues. [`poller_degraded`], [`probe_failed`], [`sequence_gap`] and
//! [`depth_repair_failed`] are the exceptions: each is written by the one
//! backend whose venue makes it possible. All four are constructed here
//! regardless, because the point of
//! this module is that the sidecar has one vocabulary — a record spelled
//! locally inside one backend is one the offline report does not know to look
//! for.
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
//! * **Ordering against the frames they explain.** Bybit's subscribe acks are
//!   venue frames bound for this same file, and market data is what a
//!   lifecycle record dates. Sending one straight to the writer would let it
//!   overtake everything still queued in front of it, so a `disconnected`
//!   could be filed ahead of frames that arrived before the socket dropped.
//!
//! This is not a claim that `_meta` is monotonic in `local_ts` overall. It is
//! not: `main` writes its own records — the minutely disk gauge, and the
//! terminal `disk_exhausted` / `stalled` / hand-off ones — straight to the
//! `Writer`, because routing them through the queue would keep a `writer_tx`
//! alive and stop `writer_rx` ever reporting that the collection task died.
//! Whenever the writer hop has a backlog, those overtake it. Anything reading
//! the sidecar has to sort.
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

/// Subscriptions the venue never acknowledged on a connection it kept open.
///
/// The record exists because this failure has no other trace. A venue that
/// serves thirteen markets of twenty-three and silently drops the rest leaves
/// a healthy socket, a healthy process, and files that simply stop for the ten
/// — measured on Lighter twice in seven days (2026-08-12, 5.19h; 2026-08-18,
/// 1.48h), and healed both times only by the next reconnect. `outstanding`
/// names them in response spelling so the sidecar says which markets were lost
/// and for how long, and `action` says what the collector did about it.
pub fn subscriptions_unacked(outstanding: &[String], waited_ms: u64, action: &str) -> Value {
    record(
        "subscriptions_unacked",
        serde_json::json!({
            "outstanding": outstanding,
            "count": outstanding.len(),
            "waited_ms": waited_ms,
            "action": action,
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

/// How long one socket has gone without serving a frame, sampled once a minute.
///
/// The ninth record, and the fourth exception to "every backend writes these":
/// only a backend that opens **several** sockets per recording can have one die
/// while the others keep the recording looking healthy, and Extended is the only
/// one that does (`extended::http::keep_connection_one`). It is spelled here for
/// the reason the whole module exists — one vocabulary for the sidecar — rather
/// than locally inside that backend.
///
/// It earns its place because neither process-level guard can see it. The stall
/// watchdog fires only on **total** silence, and the per-symbol
/// [`crate::liveness::LivenessGauge`] ages symbols, not sockets — so a dead
/// `/orderbooks` firehose, while a per-market mark socket keeps resetting both
/// clocks, is invisible to both. `served=false` is the sharp end: a socket that
/// has **never** served a frame is one whose URL the venue may be silently
/// refusing to feed, which no `dial_failed`/`disconnected` pair reports because
/// the dial itself succeeded.
pub fn socket_liveness(url: &str, age_s: u64, served: bool) -> Value {
    record(
        "socket_liveness",
        serde_json::json!({
            "url": url,
            "age_s": age_s,
            "served": served,
        }),
    )
}

/// A REST poller has been failing for long enough that its feed is missing.
///
/// The five records above are written by every backend; this one is written by
/// whichever backend runs a poller (today: `binancefuturesum`'s `premiumIndex`).
/// It is here anyway, because the vocabulary the sidecar is read with is what
/// this module is for, and a record spelled locally in one backend is a record
/// the offline gate does not know to look for.
///
/// It exists because a poller's failures are deliberately **not** fatal — the
/// data is auxiliary and ending a recording over it would be out of all
/// proportion — which leaves the journal as the only other account of them, and
/// the journal is not what an offline report reads. Raised once per outage:
/// at a ten-second cadence, a record per failure would be a sidecar nobody
/// finishes reading, and one nobody reads is one that explains nothing.
///
/// `interval_s` is not decoration. A count of consecutive failures says how
/// long the feed has been missing only once the period between them is known,
/// and the period is a constant in the backend rather than anything the record
/// would otherwise carry.
pub fn poller_degraded(
    poller: &str,
    consecutive_failures: u32,
    interval_s: u64,
    error: &str,
) -> Value {
    record(
        "poller_degraded",
        serde_json::json!({
            "poller": poller,
            "consecutive_failures": consecutive_failures,
            "interval_s": interval_s,
            "error": error,
        }),
    )
}

/// The venue would not talk to this host at all, so the collector refused to
/// start.
///
/// The eighth record, and the third exception: only a backend that probes
/// before it records writes it, which today is Lighter alone. It is spelled
/// here for the reason the module exists — one vocabulary — and it exists at
/// all because the nearest name already in that vocabulary would be a lie.
///
/// Lighter's `/stream` sits behind a jurisdiction check. From a restricted
/// region the WebSocket **upgrade** fails while REST keeps answering, so the
/// symbols resolve perfectly and the recording is empty anyway. Filing that
/// under `symbol_check_failed` would have the offline report annotate the hole
/// "explained by symbol_check_failed" and send whoever reads it to check a
/// symbol list that was never wrong. `url` is carried because that is the thing
/// that was unreachable, and it is not the endpoint the catalog came from.
pub fn probe_failed(url: &str, error: &str) -> Value {
    record(
        "probe_failed",
        serde_json::json!({
            "url": url,
            "error": error,
        }),
    )
}

/// A venue sequence number skipped ahead, so frames were lost.
///
/// The seventh record, and the second exception to "every backend writes these":
/// only a venue that publishes a sequence number can notice, and only Lighter
/// acts on one live today (`begin_nonce`..`nonce` per market, `lighter/mod.rs`).
/// It is constructed here for the reason the whole module exists — the sidecar
/// has one vocabulary, and a record spelled locally inside one backend is one
/// the offline report does not know to look for.
///
/// It earns its place because this loss is invisible everywhere else. A cadence
/// gap leaves a hole in `local_ts` that the offline report can measure; a
/// sequence break does not — the frames keep arriving on time, and only the
/// numbers inside them say that a batch in between is gone. Without this record
/// the only account of it would be the journal, which is not what an offline
/// report reads.
///
/// `count` is the market's running total in this process, so a single break and
/// a market that has been breaking all day are told apart without joining
/// records. `expected_begin_nonce` and `begin_nonce` are both carried because
/// their difference is the size of what was missed, which is the one thing a
/// consumer cannot recover from the frames it does have.
pub fn sequence_gap(
    channel: &str,
    market: i64,
    symbol: &str,
    expected_begin_nonce: u64,
    begin_nonce: u64,
    count: u64,
) -> Value {
    record(
        "sequence_gap",
        serde_json::json!({
            "channel": channel,
            "market": market,
            "symbol": symbol,
            "expected_begin_nonce": expected_begin_nonce,
            "begin_nonce": begin_nonce,
            "count": count,
        }),
    )
}

/// A break in an incremental depth feed that could NOT be repaired.
///
/// Binance USD-M publishes the book as diffs whose continuity is checked frame
/// by frame (`pu` against the previous `u`); a break is repaired by refetching
/// the whole book over REST. The repair is the only thing that makes the feed
/// recoverable, and it is the one part of the loop the venue can refuse — a
/// rate limit, a 5xx, a timeout — while the diffs keep arriving perfectly on
/// time. Offline that failure is invisible: the frames are all there, the hole
/// is inside their numbers, and a report reading the recording alone cannot
/// tell a break that was repaired from one that was not.
///
/// So the collector says so here, once per failed attempt. A break that WAS
/// repaired needs no record: the snapshot itself is in the symbol's file.
///
/// `reason` is the class — `rate_limited` when this process's own throttle
/// refused to spend the request, `fetch_failed` when the venue did — and
/// `error` carries the detail, empty for the former, which has none.
pub fn depth_repair_failed(symbol: &str, reason: &str, error: &str) -> Value {
    record(
        "depth_repair_failed",
        serde_json::json!({
            "symbol": symbol,
            "reason": reason,
            "error": error,
        }),
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
            (
                poller_degraded("premiumIndex", 30, 10, "operation timed out"),
                "poller_degraded",
            ),
            (
                probe_failed("wss://venue/stream", "the upgrade failed"),
                "probe_failed",
            ),
            (
                socket_liveness("wss://venue/orderbooks", 0, true),
                "socket_liveness",
            ),
        ] {
            assert!(is_record(&value), "{value}");
            assert_eq!(value[TAG], event, "{value}");
        }
    }

    /// The per-socket record has to carry which socket, how long it has been
    /// quiet, and — the sharp end — whether it has ever served a frame at all.
    /// A socket that never served one is the fault this exists for: the dial
    /// succeeded, so no `dial_failed` names it, and the venue is simply not
    /// feeding that URL.
    #[test]
    fn a_socket_liveness_record_names_the_socket_and_whether_it_ever_served() {
        let never = socket_liveness("wss://venue/orderbooks", 137, false);
        assert_eq!(never[TAG], "socket_liveness");
        assert_eq!(never["url"], "wss://venue/orderbooks");
        assert_eq!(never["age_s"], 137);
        assert_eq!(
            never["served"], false,
            "a socket that never served a frame must say so: {never}"
        );

        let alive = socket_liveness("wss://venue/prices/mark/BTC-USD", 3, true);
        assert_eq!(alive["served"], true);
        assert_eq!(alive["age_s"], 3);
    }

    /// A venue that will not talk to this host is not a bad symbol.
    ///
    /// The distinction is the whole point of a separate name: Lighter's
    /// `/stream` sits behind a jurisdiction check and refuses the **upgrade**
    /// while REST keeps answering, so the symbols resolved perfectly and the
    /// recording is empty anyway. Filing that as `symbol_check_failed` — the
    /// nearest existing name — would have the offline report annotate the hole
    /// "explained by symbol_check_failed" and send whoever reads it to look at
    /// the symbol list.
    #[test]
    fn an_unreachable_venue_is_not_reported_as_a_bad_symbol() {
        let refused = probe_failed(
            "wss://mainnet.zklighter.elliot.ai/stream",
            "the upgrade failed: Protocol(ResetWithoutClosingHandshake)",
        );
        assert_eq!(refused[TAG], "probe_failed");
        assert_eq!(refused["url"], "wss://mainnet.zklighter.elliot.ai/stream");
        assert!(
            refused["error"]
                .as_str()
                .unwrap()
                .contains("ResetWithoutClosingHandshake"),
            "{refused}"
        );
    }

    /// A poller that has been failing for minutes is invisible everywhere else.
    ///
    /// Its failures are warnings by design — auxiliary data must not be able to
    /// end a recording — so the journal is the only other place they appear, and
    /// a journal is not what the offline gate reads. The record has to carry
    /// enough to act on without one: which poller, how long it has been down,
    /// and what the venue last said.
    #[test]
    fn a_degraded_poller_reports_what_is_needed_to_act_on_it() {
        let degraded = poller_degraded("premiumIndex", 30, 10, "operation timed out");
        assert_eq!(degraded["poller"], "premiumIndex");
        assert_eq!(degraded["consecutive_failures"], 30);
        assert_eq!(
            degraded["interval_s"], 10,
            "30 failures means nothing without the period they were spaced by"
        );
        assert_eq!(degraded["error"], "operation timed out");
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
