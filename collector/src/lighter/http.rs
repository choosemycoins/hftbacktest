use std::{
    collections::{HashSet, VecDeque},
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::mpsc::UnboundedReceiver,
    time::{interval, timeout},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Bytes, Message, client::IntoClientRequest},
};
use tracing::{error, info, warn};

use super::{
    CLIENT_MSG_BUDGET_PER_MIN,
    MarketInfo,
    SUBSCRIBE_BUDGET_PER_MIN,
    SubscriptionSpec,
    WS_URL,
};
use crate::{
    backoff::reconnect_delay,
    meta::{self, StreamEnd},
    queue::{Frame, Tx},
};

/// How many subscribe frames go out back to back before pausing.
pub const SUBSCRIBE_CHUNK: usize = 8;

/// How long the pause between chunks is.
///
/// The per-minute budget is enforced by the market cap (`MAX_MARKETS`), not by
/// this delay — 100 frames is 100 frames whether they take one second or ten.
/// What the pause buys is the shape of the arrival: the venue's limiter sees a
/// stream rather than a wall, and its acks come back interleaved with the
/// first data instead of all at once behind it.
///
/// Short on purpose. Every second spent subscribing is a hole in some symbol's
/// file after every reconnect, so a full set has to be out in seconds — see
/// `the_whole_subscribe_set_is_out_within_seconds`.
pub const SUBSCRIBE_CHUNK_DELAY: Duration = Duration::from_millis(250);

/// How long a dial may take before it is abandoned and retried.
///
/// `connect_async` has no timeout of its own, and the failure this bounds is
/// not a refused connection but a TCP connect or TLS handshake that hangs —
/// which without a bound leaves the collector "connecting" for ever with
/// nothing in the journal after the `subscribe` record.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// How long the reachability probe waits before calling the venue unreachable.
///
/// Shorter than [`CONNECT_TIMEOUT`], because this one runs before anything is
/// recorded and its answer is "should this process exist at all".
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// A protocol-level ping every 30s.
///
/// The venue requires a client frame at least every two minutes and sends no
/// pings of its own. It answers an app-level `{"type":"ping"}` with a `pong`
/// and a protocol Ping with a protocol Pong; the protocol one is used because
/// it does not enter the recording as a frame, and 30s leaves four missed
/// pings of margin inside the venue's two minutes.
const PING_INTERVAL: Duration = Duration::from_secs(30);

/// How long a connection may deliver nothing at all before it is torn down.
///
/// A silent socket is the failure mode this venue makes easiest to reach: an
/// unknown market id is answered with an error frame and *not* a close, and a
/// throttled connection simply stops. Neither raises an error on this side.
///
/// 90s against the measured feed is a very wide margin — the slowest of the
/// four channels ran at a 1.77s worst interval on a single market
/// (2026-07-28), and this triggers only when *every* market and *every*
/// channel is silent at once.
///
/// Deliberately wide, because the two ways to be wrong are not symmetric. A
/// false positive costs a reconnect: a hole of about a second, and a fresh
/// snapshot. A false negative costs the recording — the process stays up,
/// systemd reports it healthy, and the day's file simply stops. So this is set
/// far enough out that only a genuinely dead feed reaches it, and it is a
/// **data** clock: pongs do not feed it, or a venue that answers keepalives
/// while serving nothing would reset it for ever.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// How often the idle check runs. A tick, not a timer per frame: the check is
/// against a 90s threshold and the feed peaks in the thousands of frames a
/// second, so rebuilding a deadline per frame would be pure overhead.
const IDLE_CHECK: Duration = Duration::from_secs(15);

/// The subscribe frames for one connection: every channel of every market.
///
/// Split out from [`keep_connection`] so the wire shape can be asserted
/// without a socket. The separator is the thing worth pinning: a subscription
/// is asked for with a slash (`order_book/0`) and every frame comes back with
/// a colon (`order_book:0`). Sending the colon form is answered with
/// `Invalid Channel` and no close, so the collector would sit on a healthy
/// connection recording nothing.
fn subscription_frames(
    subscriptions: &[SubscriptionSpec],
    markets: &[MarketInfo],
) -> Vec<serde_json::Value> {
    markets
        .iter()
        .flat_map(|market| {
            subscriptions.iter().map(move |spec| {
                serde_json::json!({
                    "type": "subscribe",
                    "channel": spec.topic(market.market_id),
                })
            })
        })
        .collect()
}

/// The frames that repair one market's book after a nonce break.
///
/// **Two frames, and the obvious one on its own does nothing.** Re-sending
/// `subscribe` for a channel this connection already holds is answered
/// `{"error":{"code":30003,"message":"Already Subscribed to : order_book:0"}}`
/// and no snapshot — measured against mainnet twice on 2026-07-28, snapshots
/// after the duplicate: 0. A repair built that way leaves the book on a broken
/// chain until the socket happens to drop for some other reason, which may be
/// hours, and nothing in the recording says the repair did not happen.
///
/// `unsubscribe` first, then `subscribe`, is honoured: ack at +267 ms and a
/// fresh 105 KB `subscribed/order_book` with `begin_nonce: 0` at +522 ms, with
/// both frames sent back to back and no wait for the ack in between (also
/// measured, on BTC, so the pipelining is not an ETH accident).
///
/// Only the book: `ticker`, `trade` and `market_stats` carry no chain, so there
/// is nothing about them a repair would fix, and each extra frame spends the
/// same budget.
fn repair_frames(market_id: i64) -> Vec<serde_json::Value> {
    // The order matters and the venue enforces it: the unsubscribe is what
    // makes the subscribe a fresh one rather than a duplicate.
    ["unsubscribe", "subscribe"]
        .into_iter()
        .map(|kind| {
            serde_json::json!({
                "type": kind,
                "channel": format!("order_book/{market_id}"),
            })
        })
        .collect()
}

/// How long after the last subscribe frame every subscription must be acked.
///
/// **This is the bound that closes the hole this venue is measured to have.**
/// A subscribe the venue does not serve is answered with nothing at all: no
/// error, no close, and — critically — no gap in the *other* markets, so
/// [`IDLE_TIMEOUT`] never fires and the connection looks perfectly healthy
/// while a subset of the roster records nothing. Measured on mainnet twice in
/// seven days: 2026-08-12, ten markets of twenty-three silent for 5.185h after
/// the 00:40:48Z reconnect; 2026-08-18, thirteen of twenty-three for 1.485h.
/// Both healed only when the socket happened to drop for some other reason.
/// The recording proves the mechanism: at the 12.08 reconnect `aero` was
/// acknowledged and `hype` was not, and `hype`'s next acknowledgement is
/// timestamped at the exact second its five-hour hole ends.
///
/// 60s against a measured ack latency of 267ms is a margin of 225×, and it is
/// wide for a second reason: it is also what keeps a resend inside the venue's
/// message budget. A full set is [`SUBSCRIBE_BUDGET_PER_MIN`] frames, so a
/// resend may not follow the set it repeats inside the same minute
/// (`a_resend_cannot_exceed_the_subscribe_budget`).
pub const SUBSCRIBE_ACK_GRACE: Duration = Duration::from_secs(60);

/// How many connections in a row may be abandoned for an incomplete set.
///
/// Dropping the connection is the cure for a venue that lost part of the
/// batch, and the recording says it works. It is the wrong cure for a market
/// that can never be subscribed at all — a market id the catalog offered and
/// the venue then retired, say — because there the ledger is never complete
/// and the collector would reconnect for ever, losing every market's stream
/// every two minutes to repair one that cannot be repaired.
///
/// So the drop is spent, not free: after this many consecutive abandons the
/// connection is kept and the loss is reported instead. Recording twenty-two
/// markets of twenty-three with a loud alarm beats recording none of them
/// quietly, and §3 of AGENTS.md calls that degraded venue mode rather than
/// failure.
pub const MAX_UNACKED_ABANDONS: u32 = 3;

/// The response spelling of a request channel: `order_book/0` -> `order_book:0`.
///
/// The two spellings differ in one separator and the venue is strict about
/// which belongs where — see [`subscription_frames`]. The mapping lives here
/// only, so "what was asked for" and "what is waited for" cannot drift apart.
fn response_spelling(request: &str) -> String {
    request.replacen('/', ":", 1)
}

/// Every channel this connection must see acknowledged, in response spelling.
///
/// Derived from the subscribe frames rather than from the specs, so the ledger
/// is by construction exactly the set that went out on the wire.
fn expected_acks(frames: &[serde_json::Value]) -> HashSet<String> {
    frames
        .iter()
        .filter_map(|frame| frame.get("channel").and_then(|c| c.as_str()))
        .map(response_spelling)
        .collect()
}

/// The channel a subscribe acknowledgement names, or `None` for anything else.
///
/// The venue acknowledges every one of [`CHANNELS`](super::CHANNELS) the same
/// way — `"type":"subscribed/<channel>"` carrying the payload — and says
/// nothing at all about a subscribe it drops, which is why the ack is the only
/// evidence there is.
///
/// Two frames must not be mistaken for one: `update/order_book` is the data,
/// and `{"error":{"code":30003,…}}` is the refusal of a *duplicate* subscribe,
/// which carries neither `type` nor `channel`. The `type` and the `channel`
/// are cross-checked against each other so a frame that merely mentions the
/// word cannot mark a subscription live.
///
/// The cheap `contains` first is not decoration: this runs on the read path of
/// a feed that peaks in the thousands of frames a second. The caller only asks
/// while the ledger is incomplete, so in the steady state the whole function
/// is never entered at all.
fn acked_channel(text: &str) -> Option<String> {
    if !text.contains("\"subscribed/") {
        return None;
    }
    let frame: serde_json::Value = serde_json::from_str(text).ok()?;
    let kind = frame.get("type")?.as_str()?.strip_prefix("subscribed/")?;
    let channel = frame.get("channel")?.as_str()?;
    let (named, _) = channel.split_once(':')?;
    (named == kind).then(|| channel.to_string())
}

/// What to do about subscriptions the venue has not acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckVerdict {
    /// Nothing outstanding, or the grace has not run out yet.
    Wait,
    /// Ask again for the ones that are missing.
    Resend,
    /// The venue will not serve this set here; drop the connection.
    Abandon,
    /// Abandoning has stopped working. Keep recording what there is, and say so.
    Degraded,
}

/// The decision, split out so it can be pinned without a socket.
///
/// Order matters and is fail-closed in the direction that costs data least:
/// nothing happens while frames are still going out (a set that is half sent
/// is not a set the venue has ignored), a missing ack is asked for once before
/// the connection is written off, and writing it off is bounded by
/// [`MAX_UNACKED_ABANDONS`].
fn ack_verdict(
    outstanding: usize,
    still_sending: bool,
    since_last_send: Duration,
    resends_spent: u32,
    abandons_spent: u32,
) -> AckVerdict {
    if outstanding == 0 || still_sending || since_last_send < SUBSCRIBE_ACK_GRACE {
        return AckVerdict::Wait;
    }
    if resends_spent == 0 {
        AckVerdict::Resend
    } else if abandons_spent < MAX_UNACKED_ABANDONS {
        AckVerdict::Abandon
    } else {
        AckVerdict::Degraded
    }
}

/// How long the paced subscribe takes for `total` frames.
///
/// Arithmetic rather than a measurement, so the budget test can fail on it.
fn pacing_span(total: usize) -> Duration {
    let chunks = total.div_ceil(SUBSCRIBE_CHUNK);
    SUBSCRIBE_CHUNK_DELAY * (chunks.saturating_sub(1)) as u32
}

/// Fails unless the WebSocket endpoint will actually talk to this host.
///
/// `/stream` sits behind a CloudFront jurisdiction check. From a restricted
/// region the **upgrade** fails while REST keeps working, and it fails as
/// `Protocol(ResetWithoutClosingHandshake)` — indistinguishable from a network
/// error. Without this probe the collector would take the catalog, subscribe
/// to nothing, reconnect for ever and exit 0, which is the exact shape of
/// failure the design document calls unacceptable.
///
/// It also measures the compression question once per start: this stack
/// (tungstenite 0.27) implements no `permessage-deflate` and never offers the
/// extension, so the server has nothing to accept and every frame arrives as
/// plain text — which is what makes the recorded bytes readable JSON. The
/// response header is logged rather than asserted, so the day the stack grows
/// the extension the journal says so.
pub async fn probe(url: &str) -> Result<(), anyhow::Error> {
    let request = url.into_client_request()?;
    let started = Instant::now();
    let outcome = timeout(PROBE_TIMEOUT, connect_async(request)).await;

    let hint = format!(
        "couldn't open a WebSocket to {url}. The endpoint is behind a jurisdiction \
         check, and from a restricted region the upgrade fails exactly like a network \
         error while REST keeps working — so a catalog fetch that succeeded proves \
         nothing. Check the region before assuming a network fault. A `?readonly=true` \
         endpoint exists for restricted regions; it is deliberately not used here \
         because it is a different data guarantee, and a recording made against it \
         would not be comparable."
    );

    match outcome {
        Err(_elapsed) => Err(anyhow::anyhow!(
            "{hint} (the upgrade did not complete within {PROBE_TIMEOUT:?})"
        )),
        Ok(Err(error)) => Err(anyhow::anyhow!("{hint} (the upgrade failed: {error})")),
        Ok(Ok((stream, response))) => {
            info!(
                took_ms = started.elapsed().as_millis() as u64,
                // Expected to be absent: see above. Logged so that a stack that
                // starts negotiating compression is visible in the journal
                // rather than as a surprise in the recorded bytes.
                extensions = ?response.headers().get("sec-websocket-extensions"),
                "the venue is reachable from this host"
            );
            drop(stream);
            Ok(())
        }
    }
}

/// `connected_at` is set the moment the socket comes up, and stays `None` if
/// the dial itself fails. It is an out-parameter because the dial failure
/// leaves through `?`, and the caller has to be able to tell a connection that
/// dropped from one that never existed.
async fn connect(
    url: &str,
    subscriptions: Vec<serde_json::Value>,
    ws_tx: &Tx<Frame>,
    resub_rx: &mut UnboundedReceiver<i64>,
    connected_at: &mut Option<Instant>,
    abandons_spent: &mut u32,
) -> Result<StreamEnd, anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = timeout(CONNECT_TIMEOUT, connect_async(request))
        .await
        .map_err(|_| {
            Error::from(io::Error::new(
                ErrorKind::TimedOut,
                format!("the dial did not complete within {CONNECT_TIMEOUT:?}"),
            ))
        })??;
    *connected_at = Some(Instant::now());
    meta::emit(ws_tx, meta::connected(url));

    let (mut write, mut read) = ws_stream.split();

    // One task owning both halves, rather than a writer spawned beside the
    // reader: the resubscribe channel is borrowed from the caller (it outlives
    // each connection and is re-read on the next one), and a spawned task
    // would need it by value.
    // The ledger of what must come back. Built before the frames are consumed,
    // from the frames themselves, so it is exactly what goes on the wire.
    let mut awaiting = expected_acks(&subscriptions);
    let catalogue = subscriptions.clone();
    let mut pending: VecDeque<serde_json::Value> = subscriptions.into();
    let mut last_send = Instant::now();
    let mut resends_spent: u32 = 0;
    let mut pacer = interval(SUBSCRIBE_CHUNK_DELAY);
    let mut ping = interval(PING_INTERVAL);
    ping.tick().await; // the first tick is immediate; nothing to keep alive yet
    let mut idle = interval(IDLE_CHECK);
    idle.tick().await;
    let mut last_frame = Instant::now();
    // A closed channel resolves `recv()` immediately, which without this guard
    // would spin the loop at full tilt once the parser has gone.
    let mut resub_open = true;

    loop {
        // Deliberately NOT `biased`. Reading first looks right — the socket is
        // what this loop is for — but this venue delivers thousands of frames a
        // second, and a branch order that always polls the socket first can
        // starve the keepalive under sustained load. The venue then drops the
        // connection at two minutes for a missed client frame, which would show
        // up as an inexplicable reconnect every two minutes on exactly the busy
        // days that matter. Random polling costs a few nanoseconds an iteration
        // and cannot starve anything.
        select! {
            frame = read.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    let recv_time = Utc::now();
                    last_frame = Instant::now();
                    // Guarded, not unconditional: once the ledger is complete
                    // this costs one integer comparison per frame, which is
                    // what a feed of thousands of frames a second can afford.
                    if !awaiting.is_empty()
                        && let Some(channel) = acked_channel(&text)
                    {
                        awaiting.remove(&channel);
                        if awaiting.is_empty() {
                            info!(
                                subscriptions = catalogue.len(),
                                took_ms = connected_at
                                    .map_or(0, |at| at.elapsed().as_millis() as u64),
                                "every subscription is acknowledged"
                            );
                        }
                    }
                    // A refused hand-off is terminal, whether the parser has
                    // gone or has simply stopped draining: `send` has already
                    // raised the fatal signal, and reading on would drop frames
                    // in silence.
                    if ws_tx.send((recv_time, text)).is_err() {
                        return Ok(StreamEnd::HandOffRefused);
                    }
                }
                // No binary frames are expected: this stack negotiates no
                // compression, so everything arrives as text.
                Some(Ok(Message::Binary(_))) => last_frame = Instant::now(),
                // Neither control frame touches `last_frame`, and that is the
                // whole point of the idle check.
                //
                // A pong proves the *socket* is alive. It says nothing about
                // whether the venue is still serving the subscriptions, and
                // "socket up, subscriptions silently gone" is precisely the
                // state this venue produces — it answers an unknown market with
                // an error frame and no close, and it throttles rather than
                // rejects. Counting a pong as life would disarm the check
                // against the one failure it exists for. Same distinction the
                // stall watchdog draws with `Source` (`watchdog.rs`), one layer
                // down.
                //
                // The incoming ping is not answered here either: tungstenite
                // queues the pong itself (`set_additional(Frame::pong(..))` in
                // `protocol/mod.rs`) and the write half flushes it. Sending one
                // as well would be a second pong, and every client frame is
                // spent against a budget of 200 a minute.
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(close_frame))) => {
                    warn!(?close_frame, "connection closed");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::ConnectionAborted,
                        "connection closed",
                    )));
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => return Err(Error::from(error)),
                None => break,
            },

            _ = pacer.tick(), if !pending.is_empty() => {
                for subscription in pending.drain(..SUBSCRIBE_CHUNK.min(pending.len()))
                    .collect::<Vec<_>>()
                {
                    write.send(Message::Text(subscription.to_string().into())).await?;
                }
                last_send = Instant::now();
            }

            market = resub_rx.recv(), if resub_open => match market {
                Some(market_id) => {
                    let frames = repair_frames(market_id);
                    info!(
                        market = market_id,
                        frames = frames.len(),
                        "repairing the order book after a chain break"
                    );
                    // Back to back, no wait for the ack in between: the loop
                    // cannot block on a response without starving the socket,
                    // and the venue serves the pair in order (measured).
                    for frame in frames {
                        write.send(Message::Text(frame.to_string().into())).await?;
                    }
                }
                None => resub_open = false,
            },

            _ = ping.tick() => {
                // Protocol-level, not the app-level `{"type":"ping"}`: the venue
                // answers both, and this one does not enter the recording.
                write.send(Message::Ping(Bytes::new())).await?;
            }

            _ = idle.tick() => {
                // Checked before the silence check, because the failure it
                // catches is invisible to that one: the markets the venue DID
                // serve keep `last_frame` fresh for ever.
                if !awaiting.is_empty() {
                    let mut outstanding: Vec<String> = awaiting.iter().cloned().collect();
                    outstanding.sort();
                    let waited = last_send.elapsed();
                    let verdict = ack_verdict(
                        outstanding.len(),
                        !pending.is_empty(),
                        waited,
                        resends_spent,
                        *abandons_spent,
                    );
                    let waited_ms = waited.as_millis() as u64;
                    match verdict {
                        AckVerdict::Wait => {}
                        AckVerdict::Resend => {
                            warn!(
                                outstanding = outstanding.len(),
                                markets = ?outstanding,
                                waited_ms,
                                "the venue acknowledged only part of the subscribe set; asking again"
                            );
                            meta::emit(
                                ws_tx,
                                meta::subscriptions_unacked(&outstanding, waited_ms, "resend"),
                            );
                            pending.extend(catalogue.iter().filter(|frame| {
                                frame
                                    .get("channel")
                                    .and_then(|c| c.as_str())
                                    .is_some_and(|c| awaiting.contains(&response_spelling(c)))
                            }).cloned());
                            resends_spent += 1;
                            last_send = Instant::now();
                        }
                        AckVerdict::Abandon => {
                            error!(
                                outstanding = outstanding.len(),
                                markets = ?outstanding,
                                waited_ms,
                                abandons_spent = *abandons_spent,
                                "the venue will not serve part of the subscribe set here; \
                                 dropping the connection so the next one starts clean"
                            );
                            meta::emit(
                                ws_tx,
                                meta::subscriptions_unacked(&outstanding, waited_ms, "abandon"),
                            );
                            *abandons_spent += 1;
                            return Err(Error::from(io::Error::new(
                                ErrorKind::TimedOut,
                                format!(
                                    "{} of {} subscriptions were never acknowledged",
                                    outstanding.len(),
                                    catalogue.len()
                                ),
                            )));
                        }
                        AckVerdict::Degraded => {
                            error!(
                                outstanding = outstanding.len(),
                                markets = ?outstanding,
                                waited_ms,
                                "these subscriptions have not been served across \
                                 {MAX_UNACKED_ABANDONS} connections; recording the rest"
                            );
                            meta::emit(
                                ws_tx,
                                meta::subscriptions_unacked(&outstanding, waited_ms, "degraded"),
                            );
                        }
                    }
                }

                let silent_for = last_frame.elapsed();
                if silent_for > IDLE_TIMEOUT {
                    // Not an error the socket reported — that is the point. A
                    // connection this venue has stopped serving stays open and
                    // says nothing, so the reconnect has to be driven from
                    // here or the collector records silence indefinitely.
                    return Err(Error::from(io::Error::new(
                        ErrorKind::TimedOut,
                        format!(
                            "no frame for {:.0}s on a connection that is still open; \
                             the subscription is gone or the connection is throttled",
                            silent_for.as_secs_f64()
                        ),
                    )));
                }
            }
        }
    }
    Ok(StreamEnd::Eof)
}

pub async fn keep_connection(
    subscriptions: Vec<SubscriptionSpec>,
    markets: Vec<MarketInfo>,
    mut resub_rx: UnboundedReceiver<i64>,
    ws_tx: Tx<Frame>,
) {
    let mut error_count: u32 = 0;
    let mut attempt: u64 = 0;
    // Carried across connections on purpose: it is what stops a market the
    // venue will never serve from costing every other market a reconnect
    // every two minutes for ever. See `MAX_UNACKED_ABANDONS`.
    let mut abandons_spent: u32 = 0;
    loop {
        let frames = subscription_frames(&subscriptions, &markets);
        info!(
            subscriptions = frames.len(),
            markets = markets.len(),
            chunk = SUBSCRIBE_CHUNK,
            chunk_delay_ms = SUBSCRIBE_CHUNK_DELAY.as_millis() as u64,
            paced_over_ms = pacing_span(frames.len()).as_millis() as u64,
            budget_per_min = SUBSCRIBE_BUDGET_PER_MIN,
            venue_limit_per_min = CLIENT_MSG_BUDGET_PER_MIN,
            "connecting to the Lighter WebSocket"
        );

        // Recorded before the dial, so a dial that never completes still says
        // what was going to be asked for.
        meta::emit(
            &ws_tx,
            meta::subscribe(WS_URL.as_str(), attempt, serde_json::json!(&frames)),
        );
        attempt += 1;

        // Anything asked for on the previous connection is moot: this one
        // subscribes to every market's book from scratch. Draining here rather
        // than acting on them saves a duplicate subscribe per pending repair.
        while resub_rx.try_recv().is_ok() {}

        let dial_time = Instant::now();
        let mut connected_at = None;

        match connect(
            WS_URL.as_str(),
            frames,
            &ws_tx,
            &mut resub_rx,
            &mut connected_at,
            &mut abandons_spent,
        )
        .await
        {
            Err(error) => {
                error!(?error, "websocket error");
                // A disconnect is otherwise indistinguishable from a quiet
                // market: the file just stops. A dial that never came up is a
                // different event, because there is no time-connected to report
                // for a connection that never existed.
                meta::emit(
                    &ws_tx,
                    match connected_at {
                        Some(at) => {
                            meta::disconnected(&error.to_string(), at.elapsed().as_millis() as u64)
                        }
                        None => meta::dial_failed(
                            &error.to_string(),
                            dial_time.elapsed().as_millis() as u64,
                        ),
                    },
                );
                error_count += 1;
                if dial_time.elapsed() > Duration::from_secs(30) {
                    error_count = 0;
                }

                tokio::time::sleep(reconnect_delay(error_count)).await;
            }
            Ok(end) => {
                let connected_for = connected_at.map_or(0, |at| at.elapsed().as_millis() as u64);
                if let Some(record) = meta::end_of_stream(end, connected_for) {
                    meta::emit(&ws_tx, record);
                }
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lighter::{CHANNELS, MAX_MARKETS, MarketInfo, REPAIR_COOLDOWN, SubscriptionSpec};

    /// The one frame that says a subscription is live, told apart from every
    /// frame that merely looks like it.
    ///
    /// Real wire fixtures, because this venue spells the request `order_book/0`
    /// and the response `order_book:0`, and a check written against the wrong
    /// one marks nothing and drops every connection.
    #[test]
    fn an_acknowledgement_names_the_channel_it_confirms() {
        use crate::lighter::fixtures::*;

        assert_eq!(
            acked_channel(ORDER_BOOK_SNAPSHOT_ETH).as_deref(),
            Some("order_book:0"),
            "the snapshot IS the acknowledgement"
        );
        // Everything else on that socket, none of which confirms anything.
        for (name, frame) in [
            ("the data that follows it", ORDER_BOOK_AFTER_SNAPSHOT_ETH),
            ("a refused duplicate subscribe", ERROR_ALREADY_SUBSCRIBED),
            ("the unsubscribe ack", UNSUBSCRIBED_ETH),
            ("a pong", PONG),
            ("the session handshake", CONNECTED),
            ("a channel we do not subscribe to", MARKET_STATS_ALL),
            ("an unknown market", ERROR_UNKNOWN_MARKET),
        ] {
            assert_eq!(
                acked_channel(frame),
                None,
                "{name} is not an acknowledgement"
            );
        }
    }

    /// A frame that says `subscribed/x` about channel `y` confirms neither.
    ///
    /// Cross-checking the two fields is what stops a malformed or hostile frame
    /// from marking a market live that the venue never served.
    #[test]
    fn an_acknowledgement_whose_type_and_channel_disagree_confirms_nothing() {
        assert_eq!(
            acked_channel(r#"{"channel":"ticker:7","type":"subscribed/order_book"}"#),
            None
        );
        assert_eq!(
            acked_channel(r#"{"channel":"order_book","type":"subscribed/order_book"}"#),
            None,
            "a channel with no market id names no market"
        );
    }

    /// The cheap `contains` must not be what makes the check correct.
    ///
    /// Measured on the 2026-08-12 recording: every frame carrying the literal
    /// `subscribed/` is an acknowledgement, 12 of 12 — so this frame is
    /// synthetic, and says so. It is here because the alternative is a function
    /// whose correctness rests on a fast path that was put there for speed:
    /// drop the guard for performance one day and `update/order_book` begins
    /// marking subscriptions live, silently, which is the exact class of
    /// failure this ledger exists to end.
    #[test]
    fn only_the_subscribed_prefix_confirms_not_a_frame_that_merely_carries_it() {
        assert_eq!(
            acked_channel(
                r#"{"channel":"order_book:0","type":"update/order_book","echo":"subscribed/order_book"}"#
            ),
            None
        );
    }

    /// The ledger waits for exactly the set that went out, market for market.
    #[test]
    fn the_ledger_waits_for_exactly_what_was_asked_for() {
        let markets = markets(&[("HYPE", 24), ("AERO", 42)]);
        let frames = subscription_frames(&specs(), &markets);
        let expected = expected_acks(&frames);

        assert_eq!(expected.len(), frames.len());
        for market in &markets {
            for channel in CHANNELS {
                assert!(
                    expected.contains(&format!("{channel}:{}", market.market_id)),
                    "{channel} of {} is not waited for",
                    market.symbol
                );
            }
        }
    }

    /// The request spelling and the acknowledged spelling are the same channel.
    ///
    /// Pinned against the wire fixture rather than against another constant, so
    /// the two halves cannot agree with each other and both be wrong.
    #[test]
    fn a_request_and_its_acknowledgement_are_one_channel_spelled_two_ways() {
        let asked = SubscriptionSpec::plain("order_book").topic(0);
        assert_eq!(asked, "order_book/0");
        assert_eq!(
            response_spelling(&asked),
            acked_channel(crate::lighter::fixtures::ORDER_BOOK_SNAPSHOT_ETH).unwrap()
        );
    }

    /// The regression itself: the market the venue silently dropped is the one
    /// the ledger still holds.
    ///
    /// Shaped on the measured incident of 2026-08-12 — the reconnect at
    /// 00:40:48Z acknowledged AERO and never acknowledged HYPE, and HYPE's file
    /// has literally zero records in UTC hours 01 through 04 while AERO's has
    /// twenty-four non-empty hours. Before this ledger existed nothing on the
    /// connection could tell the two apart.
    #[test]
    fn the_market_the_venue_dropped_is_the_one_left_outstanding() {
        let markets = markets(&[("HYPE", 24), ("AERO", 42)]);
        let frames = subscription_frames(&specs(), &markets);
        let mut awaiting = expected_acks(&frames);

        // Only AERO comes back. The acknowledgement is built in the shape the
        // wire fixture is pinned to by `an_acknowledgement_names_the_channel`.
        for channel in CHANNELS {
            let ack = format!(r#"{{"channel":"{channel}:42","type":"subscribed/{channel}"}}"#);
            awaiting.remove(&acked_channel(&ack).expect("a well-formed acknowledgement"));
        }

        let mut outstanding: Vec<String> = awaiting.into_iter().collect();
        outstanding.sort();
        assert_eq!(
            outstanding,
            vec!["market_stats:24", "order_book:24", "ticker:24", "trade:24"],
            "every channel of the dropped market, and nothing of the served one"
        );
    }

    /// A missing acknowledgement is asked for once, then the connection goes.
    #[test]
    fn an_unacknowledged_subscription_is_asked_once_before_the_connection_is_dropped() {
        let late = SUBSCRIBE_ACK_GRACE + Duration::from_secs(1);
        assert_eq!(ack_verdict(4, false, late, 0, 0), AckVerdict::Resend);
        assert_eq!(ack_verdict(4, false, late, 1, 0), AckVerdict::Abandon);
    }

    /// Dropping the connection is spent, not free.
    ///
    /// A market that can never be subscribed would otherwise cost every OTHER
    /// market a reconnect every two minutes for ever — worse than the hole it
    /// is trying to close.
    #[test]
    fn abandoning_is_bounded_so_one_dead_market_cannot_cost_every_other_one() {
        let late = SUBSCRIBE_ACK_GRACE + Duration::from_secs(1);
        for spent in 0..MAX_UNACKED_ABANDONS {
            assert_eq!(ack_verdict(4, false, late, 1, spent), AckVerdict::Abandon);
        }
        assert_eq!(
            ack_verdict(4, false, late, 1, MAX_UNACKED_ABANDONS),
            AckVerdict::Degraded,
            "after the budget the loss is reported, not repaired"
        );
    }

    /// Half a set is not an ignored set.
    #[test]
    fn nothing_is_asked_again_while_the_set_is_still_going_out() {
        let late = SUBSCRIBE_ACK_GRACE * 10;
        assert_eq!(ack_verdict(40, true, late, 0, 0), AckVerdict::Wait);
    }

    /// A complete ledger never speaks, and the grace is not spent early.
    #[test]
    fn a_complete_ledger_never_asks_again_and_the_grace_is_not_spent_early() {
        let late = SUBSCRIBE_ACK_GRACE + Duration::from_secs(1);
        assert_eq!(ack_verdict(0, false, late, 0, 0), AckVerdict::Wait);
        assert_eq!(
            ack_verdict(
                4,
                false,
                SUBSCRIBE_ACK_GRACE - Duration::from_millis(1),
                0,
                0
            ),
            AckVerdict::Wait
        );
    }

    /// The resend cannot put the connection over the venue's message budget.
    ///
    /// A resend is at worst the whole set again, and the whole set is the whole
    /// per-minute subscribe budget — so the two may not share a minute. That is
    /// the arithmetic reason the grace is a full minute rather than the 267ms
    /// the acknowledgement actually takes.
    #[test]
    fn a_resend_cannot_exceed_the_subscribe_budget() {
        let full_set = MAX_MARKETS * CHANNELS.len();
        assert!(full_set <= SUBSCRIBE_BUDGET_PER_MIN);
        assert!(
            SUBSCRIBE_ACK_GRACE >= Duration::from_secs(60),
            "a resend inside the same minute as the set it repeats would be throttled"
        );
    }

    fn markets(names: &[(&str, i64)]) -> Vec<MarketInfo> {
        names
            .iter()
            .map(|(s, id)| MarketInfo::test(s, *id))
            .collect()
    }

    fn specs() -> Vec<SubscriptionSpec> {
        CHANNELS
            .iter()
            .map(|c| SubscriptionSpec::plain(c))
            .collect()
    }

    /// The request spelling. A subscription is asked for with a **slash** and
    /// every frame comes back with a colon; sending the colon form gets
    /// `Invalid Channel` and a connection that stays up recording nothing.
    #[test]
    fn a_subscription_is_addressed_by_market_id_with_a_slash() {
        let frames = subscription_frames(&specs(), &markets(&[("ETH", 0)]));
        assert_eq!(
            frames[0],
            serde_json::json!({"type": "subscribe", "channel": "order_book/0"})
        );
        for frame in &frames {
            let channel = frame["channel"].as_str().unwrap();
            assert!(channel.contains('/'), "{channel}");
            assert!(!channel.contains(':'), "{channel}");
        }
    }

    /// One frame per market per channel, and no market left without the full
    /// set. A missing pairing is invisible at runtime: the venue serves what it
    /// was asked for and says nothing about the rest, which reads downstream as
    /// an instrument that was simply quiet.
    #[test]
    fn every_market_gets_every_channel() {
        let m = markets(&[("ETH", 0), ("BTC", 1), ("SOL", 2)]);
        let frames = subscription_frames(&specs(), &m);

        assert_eq!(frames.len(), CHANNELS.len() * m.len());
        for market in &m {
            for channel in CHANNELS {
                let wanted = format!("{channel}/{}", market.market_id);
                assert!(
                    frames.iter().any(|f| f["channel"] == wanted.as_str()),
                    "{} was never subscribed to {channel}",
                    market.symbol
                );
            }
        }
    }

    /// The budget arithmetic, which is the whole reason the subscribe is paced.
    ///
    /// The venue accepts 200 client messages a minute per connection and
    /// answers an over-budget client by rate-limiting it — which from this side
    /// looks like a socket that goes quiet, i.e. the failure the collector is
    /// least able to tell from a quiet market. So the subscribe set for one
    /// connection is held to half the budget, and the market cap is what
    /// enforces it (`match_catalog` refuses more).
    #[test]
    fn a_full_subscribe_set_fits_in_half_the_venue_message_budget() {
        assert!(
            SUBSCRIBE_BUDGET_PER_MIN * 2 <= CLIENT_MSG_BUDGET_PER_MIN,
            "the collector must leave the venue's budget room for the keepalives \
             and for a reconnect inside the same minute"
        );
        assert!(
            MAX_MARKETS * CHANNELS.len() <= SUBSCRIBE_BUDGET_PER_MIN,
            "{MAX_MARKETS} markets x {} channels is more than the {SUBSCRIBE_BUDGET_PER_MIN} \
             messages a connection allows itself",
            CHANNELS.len()
        );
    }

    /// Paced, but not so slowly that a reconnect leaves the last markets
    /// unsubscribed for a visible stretch of the recording. The whole set has
    /// to be out inside a few seconds — every one of those seconds is a hole in
    /// some symbol's file after every reconnect.
    #[test]
    fn the_whole_subscribe_set_is_out_within_seconds() {
        let full = MAX_MARKETS * CHANNELS.len();
        let span = pacing_span(full);

        assert!(
            span >= SUBSCRIBE_CHUNK_DELAY,
            "a full set must be paced at all, not sent in one burst: {span:?}"
        );
        assert!(
            span <= Duration::from_secs(10),
            "a reconnect would leave the last markets unsubscribed for {span:?}"
        );
    }

    /// A set that fits in one chunk is sent at once — there is nothing to pace.
    #[test]
    fn a_small_set_is_not_delayed() {
        assert_eq!(pacing_span(1), Duration::ZERO);
        assert_eq!(pacing_span(SUBSCRIBE_CHUNK), Duration::ZERO);
        assert_eq!(pacing_span(SUBSCRIBE_CHUNK + 1), SUBSCRIBE_CHUNK_DELAY);
    }

    /// **The repair is two frames, and a bare duplicate subscribe is not one.**
    ///
    /// Measured against mainnet twice on 2026-07-28. Re-sending `subscribe` on
    /// a channel this connection already holds is answered
    /// `{"error":{"code":30003,"message":"Already Subscribed to : order_book:0"}}`
    /// and **no snapshot ever follows** — snapshots before the duplicate: 1,
    /// after: 0. The book would stay on a broken chain until the socket
    /// happened to drop for some other reason, which is the one damage the
    /// whole nonce check exists to answer.
    ///
    /// `unsubscribe` then `subscribe` works, and works sent back to back
    /// without waiting for the ack in between: ack at +267 ms, a fresh 105 KB
    /// `subscribed/order_book` with `begin_nonce: 0` at +522 ms.
    #[test]
    fn a_repair_is_an_unsubscribe_then_a_subscribe() {
        assert_eq!(
            repair_frames(7),
            vec![
                serde_json::json!({"type": "unsubscribe", "channel": "order_book/7"}),
                serde_json::json!({"type": "subscribe", "channel": "order_book/7"}),
            ],
        );
    }

    /// The refusal, as a fixture, so the shape the repair must not earn is in
    /// the suite rather than only in a commit message. It names no channel, so
    /// nothing downstream can key on one — which is why the repair has to be
    /// right rather than merely observed.
    #[test]
    fn the_duplicate_subscribe_the_venue_refuses_is_pinned() {
        let refusal: serde_json::Value =
            serde_json::from_str(crate::lighter::fixtures::ERROR_ALREADY_SUBSCRIBED).unwrap();
        assert_eq!(refusal["error"]["code"], 30003);
        assert!(refusal.get("channel").is_none());

        let repair = repair_frames(0);
        assert!(
            repair.iter().any(|f| f["type"] == "unsubscribe"),
            "without the unsubscribe this is the frame the venue answers, and the \
             book is never repaired: {repair:?}"
        );
    }

    /// What the repair costs the venue's minute, which is what sets the
    /// cooldown rather than the other way round.
    ///
    /// The worst minute a connection can have: every market breaking
    /// continuously, so each one spends a full repair per cooldown, plus the
    /// keepalives, plus a reconnect inside the same minute re-subscribing
    /// everything. That has to stay inside the venue's budget, because being
    /// over it is not answered with an error — the connection is throttled,
    /// which from this side is a socket that goes quiet.
    #[test]
    fn the_worst_repair_rate_still_fits_the_message_budget() {
        let repairs_per_min = 60 / REPAIR_COOLDOWN.as_secs().max(1) as usize;
        let repair_cost = MAX_MARKETS * repair_frames(0).len() * repairs_per_min;
        let keepalives = 60 / PING_INTERVAL.as_secs().max(1) as usize;

        assert!(
            repair_cost + keepalives + SUBSCRIBE_BUDGET_PER_MIN <= CLIENT_MSG_BUDGET_PER_MIN,
            "{MAX_MARKETS} markets repairing every {REPAIR_COOLDOWN:?} costs {repair_cost} \
             messages a minute; with {keepalives} keepalives and a reconnect's \
             {SUBSCRIBE_BUDGET_PER_MIN} subscribes that is over the venue's \
             {CLIENT_MSG_BUDGET_PER_MIN}"
        );
    }
}
