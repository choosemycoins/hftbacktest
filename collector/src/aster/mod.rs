mod http;

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use chrono::{DateTime, Utc};
pub use http::{fetch_depth_snapshot, fetch_premium_index, keep_connection};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::{select, time::MissedTickBehavior};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{debug, error, info, warn};

use crate::{
    error::ConnectorError,
    file::META_STREAM,
    meta,
    pump::pump,
    queue::{Record, Tx},
    throttler::Throttler,
};

/// The streams recorded for every symbol, before `$symbol` is substituted.
///
/// A constant rather than a literal at the call site because a wrong stream
/// name is invisible at runtime: Binance accepts any name in the
/// combined-stream URL, acks it, and sends nothing — measured 2026-07-28, a
/// deliberately bogus `btcusdt@totalnonsense` behaved exactly like a stream
/// that exists and is quiet. `every_recorded_stream_name_is_well_formed` is
/// therefore the only place a typo here can be caught at all.
///
/// # Why there is no `@markPrice@1s` here, and there is on COIN-M
///
/// **The mark-price class lives on fstream's routed `/market` path, which a
/// `/public` (or legacy unrouted) connection is never served.** Binance split
/// fstream into routed classes on 2026-03-06 — `/public` carries bookTicker
/// and depth, `/market` carries aggTrade/markPrice/kline/... — and since the
/// legacy decommission date (2026-04-23) an unrouted connection is a degraded
/// alias of `/public`: subscriptions to `/market` streams are acked and then
/// never served, with no error anywhere. Measured 2026-07-28 from two
/// independent network paths before the docs explained it: every markPrice
/// variant delivered **zero** frames while `<symbol>@trade` and `!bookTicker`
/// delivered 802 frames in eight seconds on the same connection — and
/// `/market/ws/btcusdt@markPrice@1s` then delivered 8/8s once probed
/// directly. Subscribing here would need a second, `/market`-classed socket
/// with its own lifecycle; the REST poller below covers the same quantities
/// at 10s for one request per cycle, which is all a funding series needs.
/// `dstream.binance.com` (COIN-M) has not migrated and still serves
/// `markPriceUpdate` unrouted, which is why the sibling backend keeps its
/// entry — watch its changelog for the same split.
///
/// The stream sat here from 2026-07-28 morning until that measurement,
/// subscribed and silent: acked, no frames, no error, nothing in the recording.
/// Harmless but useless, and worse than useless in the list, where it read as
/// evidence that the index and funding data were being recorded. They are —
/// over REST instead, see [`PREMIUM_INDEX_INTERVAL`].
pub const STREAMS: [&str; 3] = ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@0ms"];

/// How often the premium-index snapshot is polled.
///
/// Funding, index and mark price for **basis analytics** — not first-class
/// market data. Nothing in a backtest reads it; it is recorded because a
/// perpetual's own book and tape cannot tell you afterwards what its funding
/// was priced against, and `GET /fapi/v1/premiumIndex` is the venue's own
/// answer to that. Ten seconds is a sampling choice, not a cadence the venue
/// imposes: the underlying numbers move at 1/s, the funding rate itself every
/// eight hours, and a basis series does not need per-second resolution to be
/// worth having.
///
/// The cost is one request per cycle for the whole venue — 188 KB, measured
/// 2026-07-28 across 851 symbols — of which only the recorded symbols are
/// written. That is ~1.6 GB/day of ingress and a few hundred bytes a second on
/// disk. Ingress, not disk, is what would make a shorter interval expensive.
pub const PREMIUM_INDEX_INTERVAL: Duration = Duration::from_secs(10);

/// Consecutive failed polls before the sidecar is told the feed has gone.
///
/// Thirty cycles is five minutes. Short enough that an outage is visible in the
/// day it happened, long enough that the record means "this feed is missing"
/// rather than "the venue hiccuped": a single 502 or a timed-out request is
/// ordinary and costs one sample.
const PREMIUM_INDEX_DEGRADED_AFTER: u32 = 30;

/// What this poller calls itself in the journal and in the sidecar.
const PREMIUM_INDEX_POLLER: &str = "premiumIndex";

/// Just enough of a `premiumIndex` element to decide whether to keep it.
///
/// The element itself is never deserialised into anything: it is written into
/// the recording as the bytes the venue sent, so only the routing key is read
/// out of it. `String` rather than a borrow because a symbol is 8 bytes once
/// every ten seconds, and a borrowed `&str` would additionally fail on any
/// escape sequence the venue has never yet emitted.
#[derive(Deserialize)]
struct PremiumIndexSymbol {
    symbol: String,
}

fn handle(
    prev_u_map: &mut HashMap<String, i64>,
    writer_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    data: Utf8Bytes,
    throttler: &Throttler,
) -> Result<(), ConnectorError> {
    let j: serde_json::Value = serde_json::from_str(data.as_str())?;
    // The collector's own lifecycle records travel this hop alongside the
    // venue's frames (see `meta.rs`). Matched before anything else, so one can
    // never reach the symbol routing below, which has no symbol to give it.
    if meta::is_record(&j) {
        writer_tx.send((recv_time, META_STREAM.to_string(), data.to_string()))?;
        return Ok(());
    }
    if let Some(j_data) = j.get("data")
        && let Some(j_symbol) = j_data
            .as_object()
            .ok_or(ConnectorError::FormatError)?
            .get("s")
    {
        let symbol = j_symbol.as_str().ok_or(ConnectorError::FormatError)?;
        let ev = j_data
            .get("e")
            .ok_or(ConnectorError::FormatError)?
            .as_str()
            .ok_or(ConnectorError::FormatError)?;
        if ev == "depthUpdate" {
            let u = j_data
                .get("u")
                .ok_or(ConnectorError::FormatError)?
                .as_i64()
                .ok_or(ConnectorError::FormatError)?;
            let pu = j_data
                .get("pu")
                .ok_or(ConnectorError::FormatError)?
                .as_i64()
                .ok_or(ConnectorError::FormatError)?;
            let prev_u = prev_u_map.get(symbol);
            if prev_u.is_none() || pu != *prev_u.unwrap() {
                warn!(%symbol, "missing depth feed has been detected.");
                let symbol_ = symbol.to_string();
                let writer_tx_ = writer_tx.clone();
                let mut throttler_ = throttler.clone();
                tokio::spawn(async move {
                    match throttler_.execute(fetch_depth_snapshot(&symbol_)).await {
                        Some(Ok(data)) => {
                            let recv_time = Utc::now();
                            // Detached: there is no caller to return an error
                            // to, so discarding this result would be the one
                            // silent drop the bound cannot catch. `send` has
                            // already raised the fatal signal by the time this
                            // logs — that signal is the whole error path a
                            // spawned task has.
                            if let Err(error) = writer_tx_.send((recv_time, symbol_, data)) {
                                error!(?error, "couldn't hand the depth snapshot to the writer");
                            }
                        }
                        Some(Err(error)) => {
                            error!(
                                symbol = symbol_,
                                ?error,
                                "couldn't fetch the depth snapshot."
                            );
                        }
                        None => {
                            warn!(
                                symbol = symbol_,
                                "Fetching the depth snapshot is rate-limited."
                            )
                        }
                    }
                });
            }
            *prev_u_map.entry(symbol.to_string()).or_insert(0) = u;
        }
        writer_tx.send((recv_time, symbol.to_string(), data.to_string()))?;
    }
    Ok(())
}

/// Files every element of a `premiumIndex` response that this instance records.
///
/// The element goes into the symbol's file **exactly as the venue wrote it** —
/// no envelope, no renaming, no derived fields. That is the same contract every
/// other line in the recording is written under (`queue::Record`), and it is
/// why the response is walked as [`RawValue`] rather than deserialised: a round
/// trip through `serde_json::Value` would sort the keys, because its `Map` is a
/// `BTreeMap`, and leave the file holding something no venue ever sent.
///
/// Routed under the venue's own spelling of the symbol, which is what the WS
/// frames route on too (`data.s`), so both land in one file whatever case the
/// operator typed on the command line.
fn file_premium_index(
    symbols: &HashSet<String>,
    poller_tx: &Tx<Record>,
    recv_time: DateTime<Utc>,
    body: &str,
) -> Result<usize, ConnectorError> {
    let elements: Vec<&RawValue> = serde_json::from_str(body)?;

    // Every element is read before any of them is written. A body that went
    // wrong halfway would otherwise leave the symbols ahead of the fault filed
    // and the cycle still reported as failed, and the retry ten seconds later
    // would file them again — one duplicate per symbol per cycle, for as long
    // as the venue kept answering that way.
    let mut recorded = Vec::new();
    for element in elements {
        let PremiumIndexSymbol { symbol } = serde_json::from_str(element.get())?;
        if symbols.contains(&symbol) {
            recorded.push((symbol, element));
        }
    }

    let filed = recorded.len();
    for (symbol, element) in recorded {
        // The standard fatal contract, exactly as every other producer here:
        // the poll itself may fail harmlessly, but a hand-off that cannot take
        // a record means the recording has broken. See `queue.rs`.
        poller_tx.send((recv_time, symbol, element.get().to_string()))?;
    }
    Ok(filed)
}

/// Polls the premium index until the recording breaks.
///
/// # Error policy, and why it is not the usual one
///
/// **A failed poll is a warning and a skipped cycle, never fatal.** Every other
/// failure path in this collector ends the process, and deliberately so — but
/// those are all about market data. This feed is funding and index numbers for
/// basis analytics; the order flow on the WebSocket beside it is the recording.
/// Ending a day of book and tape because a REST endpoint returned 502 would
/// trade the whole recording for the auxiliary part of it. So an HTTP error, a
/// timeout, a rate limit or a body that will not parse costs exactly one
/// sample.
///
/// **A refused hand-off to the writer is fatal, exactly as everywhere else.**
/// That is not the poller failing; it is the recording failing, and it reaches
/// `main` through the same signal every other producer raises.
///
/// The two policies need the counter between them. A failure that is only ever
/// logged is a feed that goes missing in silence — the failure mode this whole
/// collector exists to make impossible — so after
/// [`PREMIUM_INDEX_DEGRADED_AFTER`] consecutive failures the sidecar gets one
/// `poller_degraded` record, which is what the offline gate reads. One per
/// outage: at this cadence a record per failure would be a sidecar nobody
/// finishes reading. The journal still carries every individual warning.
///
/// `fetch` is a parameter so the cadence and the error policy can be tested in
/// virtual time without a network; production passes [`fetch_premium_index`].
async fn poll_premium_index<F, Fut>(
    symbols: HashSet<String>,
    poller_tx: Tx<Record>,
    mut fetch: F,
) -> Result<(), ConnectorError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<String, anyhow::Error>>,
{
    let mut ticker = tokio::time::interval(PREMIUM_INDEX_INTERVAL);
    // A slow response shifts the schedule; it must not queue ticks up behind
    // itself. The default `Burst` would fire every missed tick back to back the
    // moment a stalled request returned, turning one slow cycle into a burst of
    // requests at a venue that was evidently already struggling.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut consecutive_failures: u32 = 0;
    let mut reported = false;

    loop {
        ticker.tick().await;

        let failure = match fetch().await {
            Ok(body) => {
                let recv_time = Utc::now();
                match file_premium_index(&symbols, &poller_tx, recv_time, &body) {
                    Ok(filed) => {
                        debug!(poller = PREMIUM_INDEX_POLLER, filed, "polled");
                        None
                    }
                    // The hand-off, not the poll. `send` has already raised the
                    // process-wide signal; returning ends this task so the
                    // sender it holds is released.
                    Err(error) if error.is_fatal() => return Err(error),
                    Err(error) => Some(error.to_string()),
                }
            }
            Err(error) => Some(format!("{error:#}")),
        };

        let Some(reason) = failure else {
            consecutive_failures = 0;
            // Re-armed, so a second outage is reported as a second outage
            // rather than swallowed by the first one's record.
            reported = false;
            continue;
        };

        consecutive_failures += 1;
        warn!(
            poller = PREMIUM_INDEX_POLLER,
            consecutive_failures,
            error = reason,
            "poll failed; skipping this cycle. Index and funding data are \
             auxiliary, so this does not stop the recording"
        );

        if consecutive_failures >= PREMIUM_INDEX_DEGRADED_AFTER && !reported {
            reported = true;
            error!(
                poller = PREMIUM_INDEX_POLLER,
                consecutive_failures, "poller degraded; the feed is missing from this recording"
            );
            poller_tx.send((
                Utc::now(),
                META_STREAM.to_string(),
                meta::poller_degraded(
                    PREMIUM_INDEX_POLLER,
                    consecutive_failures,
                    PREMIUM_INDEX_INTERVAL.as_secs(),
                    &reason,
                )
                .to_string(),
            ))?;
        }
    }
}

/// Runs the two producers of this backend until either of them stops.
///
/// `select!`, and **not** a second `tokio::spawn`. `main` recognises a dead
/// collection task by a hand-off closing — i.e. by the last sender on it being
/// dropped — so a detached producer holds one open for ever and turns a dead
/// collection into a process that records nothing while systemd reports it
/// perfectly healthy. That is the trap `pump` documents for `ws_tx`, and it had
/// all five backends at once. Here the loser of the race is dropped with its
/// sender inside it.
///
/// The poller's sender is not the writer hop's, so it could no longer wedge
/// that particular door — what a detached poller would still do is keep
/// requesting from the venue on behalf of a recording that had stopped.
///
/// Extracted from `run_collection` so that invariant can be tested at all:
/// everything else in that function dials a real socket.
async fn collect_and_poll<C, P>(collect: C, poll: P) -> Result<(), anyhow::Error>
where
    C: Future<Output = Result<(), anyhow::Error>>,
    P: Future<Output = Result<(), ConnectorError>>,
{
    select! {
        collected = collect => collected,
        polled = poll => polled.map_err(anyhow::Error::from),
    }
}

/// `poller_tx` is a hand-off of its own and **not** a clone of `writer_tx`.
///
/// The two carry the same type under the same stream names into the same files,
/// and `main` has to be able to tell them apart anyway, because only one of them
/// is evidence that the venue is still sending. A `premiumIndex` element filed
/// under `BTCUSDT` is indistinguishable from a `bookTicker` frame filed under
/// `BTCUSDT` once both are in a queue; which hop it arrived on is the one thing
/// that still separates them. See `watchdog::Source` for the measurement, and
/// `queue::POLLER_QUEUE_CAPACITY` for why the hop is not sized for throughput.
pub async fn run_collection(
    streams: Vec<String>,
    symbols: Vec<String>,
    writer_tx: Tx<Record>,
    poller_tx: Tx<Record>,
) -> Result<(), anyhow::Error> {
    let mut prev_u_map = HashMap::new();
    // https://www.binance.com/en/support/faq/rate-limits-on-binance-futures-281596e222414cdd9051664ea621cdc3
    // The default rate limit per IP is 2,400/min and the weight is 20 at a depth of 1000.
    // The maximum request rate for fetching snapshots is 120 per minute.
    // Sets the rate limit with a margin to account for connection requests.
    let throttler = Throttler::new(100);

    // Uppercased once, because that is the venue's spelling in both places this
    // set has to agree with: `premiumIndex` answers `BTCUSDT`, and the WS
    // frames route on `data.s`, which is `BTCUSDT` too — whatever case the
    // operator typed, since Binance stream names are lower case and
    // `--symbols btcusdt` is a normal way to start an instance.
    let recorded: HashSet<String> = symbols.iter().map(|s| s.to_uppercase()).collect();
    info!(
        poller = PREMIUM_INDEX_POLLER,
        interval_s = PREMIUM_INDEX_INTERVAL.as_secs(),
        symbols = recorded.len(),
        "polling the venue's index and funding snapshot"
    );

    let collect = pump(
        writer_tx,
        |ws_tx| keep_connection(streams, symbols, ws_tx),
        move |writer_tx, recv_time, data| {
            handle(&mut prev_u_map, writer_tx, recv_time, data, &throttler)
        },
    );
    let poll = poll_premium_index(recorded, poller_tx, || async {
        fetch_premium_index().await.map_err(anyhow::Error::from)
    });

    collect_and_poll(collect, poll).await
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::anyhow;
    use futures_util::FutureExt;

    use super::*;
    use crate::queue::{self, WRITER_HOP};

    fn depth_update(u: i64, pu: i64) -> String {
        format!(r#"{{"data":{{"e":"depthUpdate","s":"BTCUSDT","u":{u},"pu":{pu}}}}}"#)
    }

    /// Three elements of a `GET /fapi/v1/premiumIndex` response, each captured
    /// **verbatim** from `fapi.binance.com` on 2026-07-28 — key order, spacing
    /// and number formatting exactly as the venue wrote them.
    ///
    /// That is the whole point of keeping them as strings rather than building
    /// them: the poller's contract is that the line it writes IS the element
    /// the venue sent, and a fixture assembled by `serde_json` could not catch
    /// a re-serialisation, because `serde_json::Map` is a `BTreeMap` and would
    /// silently sort the keys into a different order than the one on the wire.
    ///
    /// The live response carried 851 of these (188 KB). Three is enough to say
    /// what happens to a recorded symbol and to one that is not.
    const BTCUSDT_PREMIUM_INDEX: &str = concat!(
        r#"{"symbol":"BTCUSDT","markPrice":"63466.95207971","#,
        r#""indexPrice":"63494.85043478","estimatedSettlePrice":"63524.24373551","#,
        r#""lastFundingRate":"0.00005166","interestRate":"0.00010000","#,
        r#""nextFundingTime":1785254400000,"time":1785244313000}"#
    );
    const ETHUSDT_PREMIUM_INDEX: &str = concat!(
        r#"{"symbol":"ETHUSDT","markPrice":"1890.18000000","#,
        r#""indexPrice":"1891.28906977","estimatedSettlePrice":"1890.04113127","#,
        r#""lastFundingRate":"-0.00000878","interestRate":"0.00010000","#,
        r#""nextFundingTime":1785254400000,"time":1785244313000}"#
    );
    const DOGEUSDT_PREMIUM_INDEX: &str = concat!(
        r#"{"symbol":"DOGEUSDT","markPrice":"0.07004095","#,
        r#""indexPrice":"0.07008366","estimatedSettlePrice":"0.07010611","#,
        r#""lastFundingRate":"0.00002049","interestRate":"0.00010000","#,
        r#""nextFundingTime":1785254400000,"time":1785244313000}"#
    );

    /// The captured elements as the venue returns them: one JSON array, every
    /// symbol on the venue, no symbol parameter involved.
    fn premium_index_body() -> String {
        format!("[{BTCUSDT_PREMIUM_INDEX},{ETHUSDT_PREMIUM_INDEX},{DOGEUSDT_PREMIUM_INDEX}]")
    }

    fn recorded<const N: usize>(symbols: [&str; N]) -> HashSet<String> {
        symbols.iter().map(|s| s.to_uppercase()).collect()
    }

    /// A fetcher whose n-th call is decided by `outcome`, counting calls so a
    /// test can assert the cadence as well as the content.
    fn fetcher(
        calls: Arc<AtomicUsize>,
        outcome: impl Fn(usize) -> Result<String, anyhow::Error> + Clone,
    ) -> impl FnMut() -> std::future::Ready<Result<String, anyhow::Error>> {
        move || {
            let n = calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(outcome(n))
        }
    }

    fn meta_records(rx: &mut tokio::sync::mpsc::Receiver<Record>) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        while let Ok((_, stream, payload)) = rx.try_recv() {
            if stream == META_STREAM {
                out.push(serde_json::from_str(&payload).unwrap());
            }
        }
        out
    }

    /// The poller's whole contract in one place: parse the array, keep the
    /// symbols this instance records, file each element **as the venue wrote
    /// it** under that venue's own spelling of the symbol.
    ///
    /// Byte fidelity is not fussiness. Every other line in a symbol file is the
    /// payload exactly as it arrived (`queue::Record`, `collector/README.md`),
    /// and re-serialising through `serde_json::Value` would reorder the keys —
    /// `Map` is a `BTreeMap` — leaving the recording holding something no
    /// venue ever sent. It also keeps the poller from quietly acquiring an
    /// opinion: no envelope, no renaming, no derived fields.
    ///
    /// The venue's spelling is what routes it, and that is not the operator's:
    /// `--symbols btcusdt` is a normal way to start a UM instance because
    /// Binance stream names are lower case, while `premiumIndex` answers
    /// `BTCUSDT` and the WS frames route on `data.s`, which is also `BTCUSDT`.
    /// Filing on the configured spelling would put the poller's lines in a
    /// second file — or, after the writer lowercases, in the same file under a
    /// name that matches neither.
    #[test]
    fn premium_index_elements_are_filed_verbatim_under_the_venues_own_symbol() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let symbols = recorded(["btcusdt", "ETHUSDT"]);
        let now = Utc::now();

        let filed = file_premium_index(&symbols, &tx, now, &premium_index_body())
            .expect("a well-formed response is not a failure");

        assert_eq!(
            filed, 2,
            "the venue answers for all 851 of its symbols; only the recorded ones are written"
        );

        let (_, stream, payload) = rx.try_recv().expect("BTCUSDT is recorded");
        assert_eq!(
            stream, "BTCUSDT",
            "the venue's spelling, not the operator's"
        );
        assert_eq!(
            payload, BTCUSDT_PREMIUM_INDEX,
            "the element must reach the file byte for byte"
        );

        let (_, stream, payload) = rx.try_recv().expect("ETHUSDT is recorded");
        assert_eq!(stream, "ETHUSDT");
        assert_eq!(payload, ETHUSDT_PREMIUM_INDEX);

        assert!(
            rx.try_recv().is_err(),
            "DOGEUSDT is not recorded by this instance and must not be written"
        );
    }

    /// The same rule as every other producer. The poller's *failures* are
    /// warnings, but a refused hand-off is not a failure of the poller — it is
    /// the recording breaking — and it takes the one path that stops the
    /// process with the files closed.
    #[test]
    fn a_premium_index_element_the_writer_cannot_take_is_an_error_not_a_drop() {
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        let symbols = recorded(["BTCUSDT", "ETHUSDT"]);

        let error = file_premium_index(&symbols, &tx, Utc::now(), &premium_index_body())
            .expect_err("the second element cannot be handed over");
        assert!(matches!(error, ConnectorError::Queue(_)), "{error}");
    }

    /// A malformed body is one bad response, not a broken recording — the same
    /// rule `pump` applies to an unreadable frame. Returning the parse error
    /// lets the cycle be skipped and retried in ten seconds.
    #[test]
    fn an_unparseable_premium_index_response_is_not_fatal() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let symbols = recorded(["BTCUSDT"]);

        let error = file_premium_index(&symbols, &tx, Utc::now(), "<html>502 Bad Gateway</html>")
            .expect_err("a body that is not the documented array cannot be filed");
        assert!(!error.is_fatal(), "{error}");
        assert!(rx.try_recv().is_err(), "and nothing is written from it");
    }

    /// The cadence is the sampling rate of the recording: at the interval, one
    /// request, whatever the venue's symbol count. Ticked from a timer rather
    /// than slept in a loop, so a slow response does not make the period drift.
    ///
    /// Virtual time — five real minutes is not a test.
    #[tokio::test(start_paused = true)]
    async fn the_premium_index_poller_polls_once_per_interval() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 64);
        let calls = Arc::new(AtomicUsize::new(0));

        let _ = tokio::time::timeout(
            PREMIUM_INDEX_INTERVAL * 5 - Duration::from_millis(1),
            poll_premium_index(
                recorded(["BTCUSDT"]),
                tx,
                fetcher(calls.clone(), |_| Ok(premium_index_body())),
            ),
        )
        .await;

        assert_eq!(
            calls.load(Ordering::Relaxed),
            5,
            "one poll at startup and one per {PREMIUM_INDEX_INTERVAL:?} after it"
        );
        assert_eq!(
            (0..).take_while(|_| rx.try_recv().is_ok()).count(),
            5,
            "each poll files the one recorded symbol"
        );
    }

    /// Proportionality, stated as a test. This feed is funding and index data
    /// for basis analytics; the order flow beside it is the recording. A venue
    /// 502, a rate limit or a timed-out request must cost one sample and
    /// nothing else — ending the collection over it would trade the whole
    /// recording for the auxiliary part of it.
    #[tokio::test(start_paused = true)]
    async fn a_failed_premium_index_poll_skips_the_cycle_and_never_stops_the_recording() {
        let (tx, mut rx, mut fatal) = queue::test_bounded::<Record>(WRITER_HOP, 64);
        let calls = Arc::new(AtomicUsize::new(0));

        let outcome = tokio::time::timeout(
            PREMIUM_INDEX_INTERVAL * 4 - Duration::from_millis(1),
            poll_premium_index(
                recorded(["BTCUSDT"]),
                tx,
                fetcher(calls.clone(), |n| {
                    if n < 3 {
                        Err(anyhow!("error sending request: operation timed out"))
                    } else {
                        Ok(premium_index_body())
                    }
                }),
            ),
        )
        .await;

        assert!(
            outcome.is_err(),
            "the poller must still be running after three failures, not returned"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 4, "it kept its schedule");
        assert!(
            fatal.recv().now_or_never().is_none(),
            "a failed poll must not raise the signal that stops the process"
        );

        let (_, stream, payload) = rx.try_recv().expect("the fourth poll succeeded");
        assert_eq!(stream, "BTCUSDT");
        assert_eq!(payload, BTCUSDT_PREMIUM_INDEX);
        assert!(
            rx.try_recv().is_err(),
            "and the three failures wrote nothing at all"
        );
    }

    /// The other half of "not fatal": a failure that is never reported is a
    /// feed that goes missing in silence, which is the failure mode this whole
    /// collector is built against.
    ///
    /// So it is reported where the offline gate reads — the sidecar — and
    /// exactly once per outage. Once, because the alternative at this cadence
    /// is a record every ten seconds for as long as the venue is unreachable,
    /// and a sidecar full of them is one an operator stops reading. The journal
    /// still carries every individual warning.
    #[tokio::test(start_paused = true)]
    async fn a_persistently_failing_premium_index_poller_says_so_once() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 64);
        let calls = Arc::new(AtomicUsize::new(0));

        // Three times the threshold: long enough that a record per failure, or
        // per cycle after the threshold, would be plainly visible.
        let _ = tokio::time::timeout(
            PREMIUM_INDEX_INTERVAL * (3 * PREMIUM_INDEX_DEGRADED_AFTER),
            poll_premium_index(
                recorded(["BTCUSDT"]),
                tx,
                fetcher(calls.clone(), |_| Err(anyhow!("502 Bad Gateway"))),
            ),
        )
        .await;

        let records = meta_records(&mut rx);
        assert_eq!(
            records.len(),
            1,
            "exactly one record per outage: {records:?}"
        );
        let degraded = &records[0];
        assert_eq!(degraded["_collector"], "poller_degraded");
        assert_eq!(degraded["poller"], "premiumIndex");
        assert_eq!(
            degraded["consecutive_failures"], PREMIUM_INDEX_DEGRADED_AFTER,
            "raised at the threshold, not at some later cycle"
        );
        assert_eq!(degraded["error"], "502 Bad Gateway");
    }

    /// The poller must not be able to outlive the collection it runs beside.
    ///
    /// `main` decides that the collection task has died by watching a hand-off
    /// close, which happens only once every sender on it is gone — so a producer
    /// detached with `tokio::spawn` holds one for the life of the process and
    /// turns a dead collection into an idle one that systemd reports as healthy.
    /// That is the trap `pump` documents for `ws_tx`, and it had all five
    /// backends at once.
    ///
    /// Since the poller was given a hop of its own it is no longer the *writer*
    /// hop it could wedge open, so this is now the smaller half of that
    /// invariant rather than the whole of it — the stall watchdog is what stands
    /// behind it, and `watchdog::Source` is why the poller's own writes cannot
    /// satisfy that either. What remains true, and is what this pins, is that
    /// `select!` drops the loser with its sender inside it: a poller that
    /// outlived the collection would keep requesting from a venue for a
    /// recording that had stopped.
    #[tokio::test(start_paused = true)]
    async fn the_collection_ending_releases_the_pollers_sender() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 8);
        let poller_tx = tx.clone();

        collect_and_poll(
            // The socket loop returning, as it does when the venue closes the
            // connection cleanly.
            async move {
                drop(tx);
                Ok(())
            },
            // The poller parked on its timer holding a sender, which is what it
            // does for the whole life of a healthy process.
            async move {
                let _held = poller_tx;
                std::future::pending::<Result<(), ConnectorError>>().await
            },
        )
        .await
        .expect("a clean end of the collection is not an error here");

        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ),
            "the poller's sender must go with the poller; while one is held, \
             main cannot tell a dead collection from a live one"
        );
    }

    /// "Consecutive" has to mean consecutive. A counter that only ever went up
    /// would report a poller degraded on the strength of thirty failures spread
    /// over a day, and — having reported once — would then stay quiet through a
    /// real outage that followed.
    #[tokio::test(start_paused = true)]
    async fn one_good_poll_clears_the_count_and_re_arms_the_report() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 64);
        let calls = Arc::new(AtomicUsize::new(0));
        let recovers_at = PREMIUM_INDEX_DEGRADED_AFTER as usize;

        let _ = tokio::time::timeout(
            PREMIUM_INDEX_INTERVAL * (2 * PREMIUM_INDEX_DEGRADED_AFTER + 2),
            poll_premium_index(
                recorded(["BTCUSDT"]),
                tx,
                fetcher(calls.clone(), move |n| {
                    if n == recovers_at {
                        Ok(premium_index_body())
                    } else {
                        Err(anyhow!("502 Bad Gateway"))
                    }
                }),
            ),
        )
        .await;

        let records = meta_records(&mut rx);
        assert_eq!(
            records.len(),
            2,
            "the outage after the recovery is a second outage: {records:?}"
        );
        for degraded in &records {
            assert_eq!(
                degraded["consecutive_failures"],
                PREMIUM_INDEX_DEGRADED_AFTER
            );
        }
    }

    /// A `markPriceUpdate` frame as the combined-stream endpoint delivers it.
    ///
    /// The field set is Binance's documented USD-M one; it could not be
    /// captured here, because `fstream.binance.com` no longer serves the
    /// mark-price class of streams at all — see the note on [`STREAMS`]. The
    /// COIN-M sibling of this fixture in `binancefuturescm` was captured live
    /// and agrees on every field this test depends on.
    fn mark_price_update() -> String {
        concat!(
            r#"{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","#,
            r#""E":1785239516000,"s":"BTCUSDT","p":"63406.00000000","#,
            r#""i":"63427.35155222","P":"63402.48303662","r":"0.00005945","#,
            r#""T":1785254400000}}"#
        )
        .to_string()
    }

    /// A misspelled stream name is not an error anywhere: Binance accepts any
    /// name in the combined-stream URL, acks it, and simply never sends for it.
    /// Measured 2026-07-28 — `btcusdt@totalnonsense` connected and delivered
    /// zero frames in eight seconds, which is indistinguishable from a stream
    /// that exists and is quiet. Nothing but this assertion can catch that class
    /// of typo before it reaches a recording.
    ///
    /// The list is pinned whole rather than only shape-checked, because the
    /// same silence is what a *removal* would look like. `$symbol@markPrice@1s`
    /// was here until 2026-07-28 and was subscribed-and-silent for its whole
    /// life on this endpoint; putting it back would not restore the feed, it
    /// would only restore the appearance of one. The index and funding data now
    /// come from the REST poller — see [`PREMIUM_INDEX_INTERVAL`].
    #[test]
    fn every_recorded_stream_name_is_well_formed() {
        assert_eq!(
            STREAMS,
            ["$symbol@trade", "$symbol@bookTicker", "$symbol@depth@0ms"],
            "the UM combined stream carries order flow and nothing else: the \
             mark-price class delivers zero frames here (measured 2026-07-28) \
             and its data is polled over REST instead"
        );
        for stream in STREAMS {
            assert!(
                stream.starts_with("$symbol@"),
                "{stream}: every stream is per-symbol and the placeholder is substituted verbatim"
            );
            assert!(
                !stream.contains("@@"),
                "{stream}: a doubled `@` is silently accepted by the venue and records nothing"
            );
        }
    }

    /// Mark-price frames have to reach the symbol's file, and they have to
    /// leave the depth-gap detector alone on the way.
    ///
    /// Kept after the subscription was dropped, deliberately. Nothing about the
    /// frame handling depended on the subscription: the routing exists because
    /// `handle` files anything carrying `s`, and the `e == "depthUpdate"` guard
    /// is what keeps a non-depth frame out of the sequence logic. If USD-M ever
    /// serves this class again — the stream names are still documented, and the
    /// silence was measured from one vantage point on one day — the frames must
    /// still land in the right file rather than take the collector down.
    ///
    /// They carry `s` but neither `u` nor `pu`, so they route by symbol like
    /// everything else. Without the guard, every one of them would fail to find
    /// `u` and leave through `FormatError`, killing the collector once a second;
    /// with a laxer guard they would reset `prev_u_map` and make the next
    /// genuine depth frame look like a gap, firing a REST snapshot refetch per
    /// second against a 100/min throttle.
    #[test]
    fn mark_price_frames_are_filed_under_their_symbol_and_leave_gap_detection_alone() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 4);
        let mut prev_u_map = HashMap::from([("BTCUSDT".to_string(), 100)]);
        let throttler = Throttler::new(100);
        let now = Utc::now();

        handle(
            &mut prev_u_map,
            &tx,
            now,
            mark_price_update().as_str().into(),
            &throttler,
        )
        .expect("a mark-price frame is ordinary market data, not a parse failure");

        let (_, stream, payload) = rx.try_recv().expect("the frame must be written");
        assert_eq!(
            stream, "BTCUSDT",
            "it belongs to the symbol, not the sidecar"
        );
        assert!(
            payload.contains(r#""i":"63427.35155222""#),
            "the index price is the whole reason this stream is recorded"
        );
        assert_eq!(
            prev_u_map.get("BTCUSDT"),
            Some(&100),
            "a non-depth frame must not touch the depth sequence state"
        );
    }

    /// The same rule at the Binance call site, which is also the one holding
    /// the detached REST snapshot task. That task has no caller to return an
    /// error to at all, so its only route out is the fatal signal `Tx::send`
    /// raises — which is why the result may not be discarded here either.
    #[test]
    fn a_frame_the_writer_cannot_take_is_an_error_not_a_drop() {
        let (tx, _rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 1);
        // Primed so both frames continue the sequence: a gap would spawn the
        // REST snapshot fetch, which is not what this test is about.
        let mut prev_u_map = HashMap::from([("BTCUSDT".to_string(), 1)]);
        let throttler = Throttler::new(100);
        let now = Utc::now();

        handle(
            &mut prev_u_map,
            &tx,
            now,
            depth_update(2, 1).as_str().into(),
            &throttler,
        )
        .expect("the first frame fits");

        let error = handle(
            &mut prev_u_map,
            &tx,
            now,
            depth_update(3, 2).as_str().into(),
            &throttler,
        )
        .expect_err("a frame that could not be handed over must not be reported as written");
        assert!(matches!(error, ConnectorError::Queue(_)), "{error}");
    }

    /// The collector's own lifecycle records ride the same hop as the venue's
    /// frames, so `handle` is both what files them in the sidecar and what
    /// keeps them out of the symbol files — this backend routes on a symbol
    /// parsed out of the frame, and a lifecycle record has none. Until they
    /// were wired in, a Binance recording wrote nothing to `_meta` at all and
    /// so could not explain a single gap.
    #[test]
    fn lifecycle_records_are_filed_under_meta_and_market_data_still_is_not() {
        let (tx, mut rx, _fatal) = queue::test_bounded::<Record>(WRITER_HOP, 4);
        let mut prev_u_map = HashMap::new();
        let throttler = Throttler::new(100);
        let now = Utc::now();
        let lifecycle = meta::disconnected("Connection reset without closing handshake", 1194);

        handle(
            &mut prev_u_map,
            &tx,
            now,
            lifecycle.to_string().as_str().into(),
            &throttler,
        )
        .unwrap();
        handle(
            &mut prev_u_map,
            &tx,
            now,
            r#"{"data":{"e":"trade","s":"BTCUSDT"}}"#.into(),
            &throttler,
        )
        .unwrap();

        let (_, stream, payload) = rx.try_recv().expect("the lifecycle record must be written");
        assert_eq!(stream, META_STREAM);
        let j: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(j["_collector"], "disconnected");
        assert_eq!(j["connected_for_ms"], 1194);

        assert_eq!(
            rx.try_recv().expect("market data must still be written").1,
            "BTCUSDT",
            "the symbol routing below the tag check is untouched"
        );
    }
}
