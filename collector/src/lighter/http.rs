use std::{
    collections::VecDeque,
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
    subscription::{ADMITTED_BEFORE_REFUSAL, Subscriptions, Verdict, abandon_budget, on_idle_tick},
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

/// How many frames go out per tick once the venue has refused one for rate.
///
/// One, and the tick lengthens with it — see [`THROTTLED_CHUNK_DELAY`]. The
/// burst rate is what caused the refusal (measured: 38 frames admitted, then
/// refusals from the 39th at +1.01s), so continuing at it would refuse the
/// rest of the queue too.
const THROTTLED_CHUNK: usize = 1;

/// The tick between frames once throttled: 2.5 a second.
///
/// Under the venue's published 200 client messages a minute (3.33/s) with
/// margin, because that budget has to cover the keepalive and any book repair
/// as well. A full 92-frame set at this pace takes 37s, which is why it is the
/// fallback and not the normal path: every second spent subscribing is a hole
/// in some symbol's file.
const THROTTLED_CHUNK_DELAY: Duration = Duration::from_millis(400);

/// How many subscribe frames go out per tick, given whether the venue has
/// refused one for rate.
const fn chunk_for(throttled: bool) -> usize {
    if throttled {
        THROTTLED_CHUNK
    } else {
        SUBSCRIBE_CHUNK
    }
}

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
    // The ledger of what must come back — see `subscription.rs` for why the
    // acknowledgement, and not the silence, is what this connection watches.
    let mut ledger = Subscriptions::new(&subscriptions);
    let mut pending: VecDeque<serde_json::Value> = subscriptions.into();
    let mut last_send = Instant::now();
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
                    if !ledger.is_complete() {
                        let was_throttled = ledger.throttled();
                        ledger.observe(&text);
                        if ledger.throttled() && !was_throttled {
                            // The venue is refusing our own burst. Slow the
                            // pacer for the rest of this connection: the frames
                            // still queued would otherwise be refused too.
                            warn!(
                                outstanding = ledger.outstanding().len(),
                                still_queued = pending.len(),
                                measured_admission_limit = ADMITTED_BEFORE_REFUSAL,
                                "the venue is refusing subscribe frames for rate; \
                                 pacing the rest at the sustained rate"
                            );
                            pacer = interval(THROTTLED_CHUNK_DELAY);
                        }
                        if ledger.is_complete() {
                            *abandons_spent = abandon_budget(*abandons_spent, &ledger);
                            info!(
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
                let chunk = chunk_for(ledger.throttled());
                for subscription in pending.drain(..chunk.min(pending.len()))
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
                if !ledger.is_complete() {
                    let waited = last_send.elapsed();
                    let waited_ms = waited.as_millis() as u64;
                    let (verdict, spent) =
                        on_idle_tick(&mut ledger, pending.len(), waited, *abandons_spent);
                    *abandons_spent = spent;
                    match verdict {
                        Verdict::Wait => {}
                        Verdict::Resend(frames) => {
                            let outstanding = ledger.outstanding();
                            warn!(
                                outstanding = outstanding.len(),
                                markets = ?outstanding,
                                waited_ms,
                                throttled = ledger.throttled(),
                                "the venue acknowledged only part of the subscribe set; \
                                 asking again"
                            );
                            meta::emit(
                                ws_tx,
                                meta::subscriptions_unacked(&outstanding, waited_ms, "resend"),
                            );
                            pending.extend(frames);
                            last_send = Instant::now();
                        }
                        Verdict::Abandon(outstanding) => {
                            error!(
                                outstanding = outstanding.len(),
                                markets = ?outstanding,
                                waited_ms,
                                abandons_spent = *abandons_spent,
                                "the venue is not serving part of the subscribe set and \
                                 has not said why; dropping the connection"
                            );
                            meta::emit(
                                ws_tx,
                                meta::subscriptions_unacked(&outstanding, waited_ms, "abandon"),
                            );
                            return Err(Error::from(io::Error::new(
                                ErrorKind::TimedOut,
                                format!(
                                    "{} subscriptions were never acknowledged",
                                    outstanding.len()
                                ),
                            )));
                        }
                        Verdict::Report(outstanding) => {
                            error!(
                                outstanding = outstanding.len(),
                                markets = ?outstanding,
                                waited_ms,
                                throttled = ledger.throttled(),
                                "these subscriptions are not being served; recording the rest"
                            );
                            meta::emit(
                                ws_tx,
                                meta::subscriptions_unacked(&outstanding, waited_ms, "report"),
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

    /// Once the venue has refused a frame for rate, the pace changes — and it
    /// changes to something under the venue's own published sustained budget.
    ///
    /// Without this the ledger would notice the refusal, log it, and keep
    /// firing the rest of the queue at exactly the rate that caused it.
    #[test]
    fn a_refused_burst_slows_the_pacer_below_the_sustained_budget() {
        assert_eq!(chunk_for(false), SUBSCRIBE_CHUNK);
        assert_eq!(chunk_for(true), THROTTLED_CHUNK);
        assert!(chunk_for(true) < chunk_for(false));

        let throttled_per_min =
            chunk_for(true) as f64 * (60.0 / THROTTLED_CHUNK_DELAY.as_secs_f64());
        assert!(
            throttled_per_min < CLIENT_MSG_BUDGET_PER_MIN as f64,
            "the throttled pace ({throttled_per_min:.0}/min) has to leave room for the \
             keepalive and a book repair inside {CLIENT_MSG_BUDGET_PER_MIN}/min"
        );
        let burst_per_min = chunk_for(false) as f64 * (60.0 / SUBSCRIBE_CHUNK_DELAY.as_secs_f64());
        assert!(
            burst_per_min > CLIENT_MSG_BUDGET_PER_MIN as f64,
            "and the normal pace is over it, which is why the throttled one exists"
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
