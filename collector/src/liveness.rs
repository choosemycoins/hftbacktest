//! How long each symbol has been silent, sampled once a minute into `_meta`.
//!
//! The stall watchdog is a floor under *everything*: it fires only when nothing
//! at all reaches disk, and `watchdog.rs` is explicit that it will not notice
//! "one symbol of ten that stopped". Neither will anything else in the process.
//! Until the next day's offline report ran, a coin whose subscription was
//! quietly dropped looked exactly like a coin nobody traded.
//!
//! This closes the per-symbol half of that. It is not a second watchdog and it
//! never stops the collector: partial silence is ambiguous in a way total
//! silence is not — a thin symbol in a quiet hour really can go a minute
//! without a print — so it warns and records, and the decision stays with the
//! operator and the offline gate.
//!
//! ## Per symbol, not per symbol × stream
//!
//! A deliberate limit, and the reason is where this sits. The only thing the
//! writer hop carries is `queue::Record` — `(recv_time, symbol, payload)` — and
//! the payload is the venue's bytes, opaque; the stream it belongs to exists
//! nowhere on this path.
//!
//! It is *not* that recovering it would cost a parse. Every backend's `handle`
//! already does one `serde_json::from_str` per frame and already holds the
//! answer while it does — Bybit's `topic`, Hyperliquid's `channel`, Binance's
//! `data.e` — so the classification is free at the producer. What it would cost
//! is carrying it: `Record`'s third field would become a fourth, which is an
//! extra owned `String` allocated and moved across the hop **per record** at
//! the peak rate this collector has measured (~20 000 msg/s, `queue.rs`), for a
//! number read once a minute. Avoiding that allocation means interning the
//! stream names into an id, which is a change to the queue's element type and
//! to five parsers.
//!
//! So the reason is a per-record allocation and a five-backend change, not a
//! parse — worth being exact about, because the parse argument would also rule
//! out doing it properly later, and it does not.
//!
//! So: per symbol, at the cost of one `Instant` write into a short list per
//! record. That already catches the case this was built for — one coin went
//! quiet — and it does not catch "depth died while trades flow", which stays
//! the offline report's job and is documented as such in `README.md`. Buying
//! the second half is a real change with a real cost, and it should be made
//! knowingly rather than by accident.
//!
//! ## Why a `Vec` and not a `HashMap`
//!
//! Because the alternative allocates on the hot path. The venue spells symbols
//! its own way (`BTCUSDT`) and the operator spells them theirs, so the two have
//! to be matched case-insensitively; with a map that means folding the case of
//! every record's symbol to build a key, which is an allocation per record.
//! A linear scan with `eq_ignore_ascii_case` needs none, and the list is the
//! subscribed symbol set — ten or twenty entries, compared against a few
//! hundred bytes of JSON that the writer is about to gzip anyway.

use std::time::Duration;

use serde_json::Value;
use tokio::time::Instant;

use crate::{file::META_STREAM, meta, watchdog::Source};

/// The most symbols tracked at once.
///
/// The subscribed set is the real bound — every recorded symbol also gets an
/// open file, so a venue inventing them without limit exhausts the fd table
/// long before this matters. The cap exists so the linear scan above stays a
/// scan over a short list no matter what arrives, since that is the whole
/// reason it can be a scan.
///
/// Hitting it puts `"overflowed": true` in the record and produces no log line.
/// Symbols nobody subscribed to are what the offline report's
/// `unexpected_symbol` check is for, and it can see the whole day; a warning
/// here would fire every minute for a condition this process cannot act on.
pub const MAX_TRACKED_SYMBOLS: usize = 256;

/// The per-symbol silence a venue can produce without anything being wrong, in
/// seconds.
///
/// Derived from the slowest feed the venue serves **per symbol**, because any
/// one of a symbol's streams arriving resets its clock. Two venues carry a
/// periodic per-symbol feed — one that arrives whether or not the market moved
/// — and can therefore be held to something far tighter than the rest:
///
/// * `hyperliquid` — `activeAssetCtx` at ~1.018 s per coin, and it is in
///   [`crate::hyperliquid::ALWAYS_ON`], so no combination of `--hl-l2-modes`
///   can take it away. 60 s is sixty times the measured cadence and six times
///   the limit the offline report holds the same stream to.
/// * `binancefuturescm` — the same, through `$symbol@markPrice@1s` at ~1.000 s
///   (measured 2026-07-28, `README.md`).
///
/// Everything else records order flow only, where silence is legal: USD-M lost
/// the whole markPrice class of streams, its `premiumIndex` replacement is the
/// collector's own poller and deliberately does not count (see [`Source`]), and
/// Bybit and spot Binance never had a periodic feed. There the slowest stream
/// is the trade tape, which the offline report gives 120 s; 300 s is two and a
/// half times that, and the same order as `--stall-timeout-min` — but applied
/// per symbol, which is exactly the case the watchdog cannot see.
///
/// An unknown name gets the conservative number. `main` rejects unknown
/// exchanges before this is reached, so it only decides what a future backend
/// gets before someone measures it: a threshold too loose costs a late warning,
/// one too tight costs a false alarm every day, and only the second makes the
/// gauge worthless.
pub fn default_threshold_s(exchange: &str) -> u64 {
    match exchange {
        "hyperliquid" | "binancefuturescm" => 60,
        _ => 300,
    }
}

/// A change of state worth a log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Transition {
    /// Nothing has been recorded for this symbol for longer than the threshold.
    WentQuiet { symbol: String, age_s: u64 },
    /// It started arriving again.
    Resumed { symbol: String },
}

struct Tracked {
    /// The spelling this symbol was first seen under — the operator's for a
    /// subscribed symbol, the venue's for anything else.
    name: String,
    last: Instant,
    /// Whether it is currently in the warned state, so the warning is raised on
    /// the edge rather than once a minute for as long as the silence lasts.
    quiet: bool,
}

/// Ages every symbol, and warns on the ones that stop.
pub struct LivenessGauge {
    /// `None` disables the warning. The gauge still records ages: the record is
    /// evidence for a later manifest, and the alarm is a convenience on top of
    /// it, so turning off the alarm must not turn off the evidence.
    threshold: Option<Duration>,
    tracked: Vec<Tracked>,
    /// Whether the cap has been hit, so it is reported once rather than per
    /// record.
    overflowed: bool,
}

impl LivenessGauge {
    /// `threshold_s` of `0` disables the warning.
    ///
    /// Seeded with the symbols that were asked for, aged from now. A symbol
    /// whose subscription the venue silently dropped never reaches
    /// [`Self::record_write`] at all, so a gauge that only knew about symbols
    /// it had seen would be blind to the worst version of the fault — a
    /// partially accepted subscription, which `watchdog.rs` lists among the
    /// things nothing catches. Same reasoning as the watchdog starting its
    /// clock at construction rather than at the first record.
    pub fn new(threshold_s: u64, symbols: &[String]) -> Self {
        let now = Instant::now();
        Self {
            // Seconds straight from the command line, so no arithmetic can
            // overflow here: an absurd value gives an unreachable threshold
            // rather than a panic, and the release profile aborts on panic —
            // one here would take the day's unfinished gzip members with it.
            threshold: (threshold_s > 0).then(|| Duration::from_secs(threshold_s)),
            tracked: symbols
                .iter()
                .take(MAX_TRACKED_SYMBOLS)
                .map(|name| Tracked {
                    name: name.clone(),
                    last: now,
                    quiet: false,
                })
                .collect(),
            // A subscription list past the cap is truncated here, and that must
            // be visible: the symbols dropped are ones the operator explicitly
            // asked for, so a record that did not say so would report on a
            // subset while looking complete.
            overflowed: symbols.len() > MAX_TRACKED_SYMBOLS,
        }
    }

    /// Notes a record on its way to disk. Called per record, on the hot path.
    ///
    /// Takes the same [`Source`] as the stall watchdog and applies the same two
    /// exclusions, for the same reasons and in the same place: the sidecar is
    /// not market data, and a record the collector produced on a timer of its
    /// own is evidence the timer is running, not that the venue is sending. On
    /// USD-M the `premiumIndex` poller files under `BTCUSDT` exactly as a
    /// `bookTicker` frame does, so counting it would report every symbol alive
    /// with the socket dead — measured, see [`Source`].
    ///
    /// No warning is decided here. This does one comparison per tracked symbol
    /// and one write; the threshold is applied in [`Self::sample`], once a
    /// minute.
    pub fn record_write(&mut self, source: Source, symbol: &str) {
        if source != Source::Venue || symbol == META_STREAM {
            return;
        }
        let now = Instant::now();
        for entry in &mut self.tracked {
            // ASCII-case-insensitive, so the venue's `BTCUSDT` and the
            // `btcusdt` the operator typed are one symbol rather than two —
            // and the second of them permanently silent. This is also what
            // `Writer` does to decide a filename.
            if entry.name.eq_ignore_ascii_case(symbol) {
                entry.last = now;
                return;
            }
        }
        if self.tracked.len() < MAX_TRACKED_SYMBOLS {
            self.tracked.push(Tracked {
                name: symbol.to_string(),
                last: now,
                quiet: false,
            });
        } else {
            self.overflowed = true;
        }
    }

    /// The minutely record, plus whichever symbols changed state since the last
    /// one.
    pub fn sample(&mut self) -> (Value, Vec<Transition>) {
        let now = Instant::now();
        let mut ages = serde_json::Map::new();
        let mut transitions = Vec::new();

        for entry in &mut self.tracked {
            let age = now.saturating_duration_since(entry.last);
            ages.insert(entry.name.clone(), Value::from(age.as_secs()));

            let Some(threshold) = self.threshold else {
                continue;
            };
            match (age > threshold, entry.quiet) {
                (true, false) => {
                    entry.quiet = true;
                    transitions.push(Transition::WentQuiet {
                        symbol: entry.name.clone(),
                        age_s: age.as_secs(),
                    });
                }
                (false, true) => {
                    entry.quiet = false;
                    transitions.push(Transition::Resumed {
                        symbol: entry.name.clone(),
                    });
                }
                _ => {}
            }
        }

        let mut fields = serde_json::Map::new();
        fields.insert(meta::TAG.to_string(), Value::from("liveness"));
        // Carried in the record so a reading of it does not have to know which
        // collector version, venue default or command-line flag was in force.
        fields.insert(
            "threshold_s".to_string(),
            Value::from(self.threshold.map(|t| t.as_secs()).unwrap_or(0)),
        );
        if self.overflowed {
            fields.insert("overflowed".to_string(), Value::from(true));
        }
        fields.insert("ages_s".to_string(), Value::Object(ages));
        (Value::Object(fields), transitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THRESHOLD_S: u64 = 60;

    fn symbols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn ages(record: &Value) -> &serde_json::Map<String, Value> {
        record["ages_s"].as_object().unwrap()
    }

    /// The failure this exists for. Nine symbols keep arriving, one stops. The
    /// stall watchdog is satisfied — data is reaching disk — and nothing else
    /// in the process looks at symbols individually, so before this the fault
    /// surfaced in the next day's offline report or not at all.
    #[tokio::test(start_paused = true)]
    async fn one_quiet_symbol_is_reported_while_the_others_keep_arriving() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &symbols(&["BTC", "ETH", "SOL"]));

        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(10)).await;
            gauge.record_write(Source::Venue, "BTC");
            gauge.record_write(Source::Venue, "ETH");
        }

        let (record, transitions) = gauge.sample();

        assert_eq!(
            transitions,
            vec![Transition::WentQuiet {
                symbol: "SOL".to_string(),
                age_s: 100,
            }],
            "the quiet symbol was not the one reported: {record}"
        );
        assert_eq!(ages(&record)["BTC"], 0);
        assert_eq!(ages(&record)["ETH"], 0);
        assert_eq!(ages(&record)["SOL"], 100);
    }

    /// The worst version of the fault is a symbol that never arrives at all — a
    /// partially accepted subscription, which `watchdog.rs` lists among the
    /// things nothing in the process catches. A gauge that only knew about
    /// symbols it had already seen would have no entry for it and report
    /// nothing.
    #[tokio::test(start_paused = true)]
    async fn a_symbol_that_never_arrives_is_visible_from_startup() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &symbols(&["BTC", "NEVER"]));

        tokio::time::advance(Duration::from_secs(THRESHOLD_S + 1)).await;
        gauge.record_write(Source::Venue, "BTC");

        let (record, transitions) = gauge.sample();

        assert_eq!(
            transitions,
            vec![Transition::WentQuiet {
                symbol: "NEVER".to_string(),
                age_s: THRESHOLD_S + 1,
            }]
        );
        assert!(
            ages(&record).contains_key("NEVER"),
            "a symbol that never arrived is missing from the gauge: {record}"
        );
    }

    /// Edge-triggered per symbol. A subscription that was dropped stays dropped
    /// until someone restarts the collector, and a warning a minute per symbol
    /// for the hours that takes is a journal in which nothing else can be seen.
    #[tokio::test(start_paused = true)]
    async fn the_warning_is_raised_once_per_symbol_and_cleared_once() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &symbols(&["BTC"]));

        tokio::time::advance(Duration::from_secs(THRESHOLD_S + 1)).await;
        let (_, first) = gauge.sample();
        assert_eq!(
            first,
            vec![Transition::WentQuiet {
                symbol: "BTC".to_string(),
                age_s: THRESHOLD_S + 1,
            }]
        );

        tokio::time::advance(Duration::from_secs(60)).await;
        let (_, again) = gauge.sample();
        assert_eq!(again, vec![], "warned twice about one silence");

        gauge.record_write(Source::Venue, "BTC");
        let (_, recovered) = gauge.sample();
        assert_eq!(
            recovered,
            vec![Transition::Resumed {
                symbol: "BTC".to_string()
            }]
        );

        let (_, quiet) = gauge.sample();
        assert_eq!(quiet, vec![], "reported a recovery twice");
    }

    /// The collector's own periodic output must not vouch for a symbol, and the
    /// case that makes it load-bearing is USD-M's `premiumIndex` poller: it
    /// files under `BTCUSDT` exactly as a `bookTicker` frame does, and it keeps
    /// running with the socket dead. Counting it would report every symbol
    /// alive during precisely the outage this is for — the same measurement
    /// that disarmed the stall watchdog, see `watchdog::Source`.
    #[tokio::test(start_paused = true)]
    async fn the_collectors_own_records_do_not_vouch_for_a_symbol() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &symbols(&["BTCUSDT"]));

        for _ in 0..7 {
            tokio::time::advance(Duration::from_secs(10)).await;
            gauge.record_write(Source::Collector, "BTCUSDT");
        }

        let (_, transitions) = gauge.sample();

        assert_eq!(
            transitions,
            vec![Transition::WentQuiet {
                symbol: "BTCUSDT".to_string(),
                age_s: 70,
            }],
            "a poller running against a dead socket held the gauge open"
        );
    }

    /// Sidecar records travel the same hop as market data and reach here under
    /// `_meta` (`meta.rs`). During a reconnect storm that is a couple a second
    /// indefinitely — the case the stall watchdog documents — and counting them
    /// would keep every symbol looking alive while nothing is recorded.
    ///
    /// It is filed under one name and no symbol, so it must also not become a
    /// tracked entry of its own.
    #[tokio::test(start_paused = true)]
    async fn sidecar_records_are_neither_life_nor_a_symbol() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &symbols(&["BTC"]));

        for _ in 0..14 {
            tokio::time::advance(Duration::from_secs(5)).await;
            gauge.record_write(Source::Venue, META_STREAM);
        }

        let (record, transitions) = gauge.sample();

        assert_eq!(transitions.len(), 1, "a reconnect storm counted as life");
        assert!(
            !ages(&record).contains_key(META_STREAM),
            "the sidecar became a tracked symbol: {record}"
        );
    }

    /// The operator types one spelling and the venue sends another — Bybit
    /// `BTCUSDT` against a command line of `btcusdt`, and Hyperliquid's own
    /// casing for a coin. Matching them literally would leave the seeded entry
    /// permanently silent and warn about it every run, which is the one failure
    /// mode that makes a gauge worthless: a warning that is always there.
    #[tokio::test(start_paused = true)]
    async fn the_venues_spelling_and_the_operators_are_one_symbol() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &symbols(&["btcusdt"]));

        tokio::time::advance(Duration::from_secs(THRESHOLD_S + 1)).await;
        gauge.record_write(Source::Venue, "BTCUSDT");

        let (record, transitions) = gauge.sample();

        assert_eq!(
            transitions,
            vec![],
            "one symbol was tracked as two: {record}"
        );
        assert_eq!(ages(&record).len(), 1, "{record}");
        assert_eq!(ages(&record)["btcusdt"], 0);
    }

    /// `0` disables the *warning*, not the measurement. The record is what a
    /// dataset manifest reads afterwards, and an operator who silenced a noisy
    /// alarm has not asked to stop recording what the collector saw.
    #[tokio::test(start_paused = true)]
    async fn zero_disables_the_warning_but_not_the_gauge() {
        let mut gauge = LivenessGauge::new(0, &symbols(&["BTC"]));

        tokio::time::advance(Duration::from_secs(3600)).await;
        let (record, transitions) = gauge.sample();

        assert_eq!(transitions, vec![], "a disabled threshold still warned");
        assert_eq!(record["threshold_s"], 0);
        assert_eq!(
            ages(&record)["BTC"],
            3600,
            "the gauge stopped measuring when the alarm was turned off: {record}"
        );
    }

    /// The record has to be readable by the same parser as every other
    /// `_collector` record, and self-describing: a manifest reading ages a
    /// month later cannot know which venue default or flag was in force.
    #[tokio::test(start_paused = true)]
    async fn the_record_is_tagged_and_carries_the_threshold_it_was_judged_by() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &symbols(&["BTC", "ETH"]));

        let (record, _) = gauge.sample();

        assert!(meta::is_record(&record), "{record}");
        assert_eq!(record[meta::TAG], "liveness");
        assert_eq!(record["threshold_s"], THRESHOLD_S);
        assert_eq!(ages(&record).len(), 2, "{record}");
        assert!(
            record.get("overflowed").is_none(),
            "the ordinary case carries the cap flag: {record}"
        );
    }

    /// The scan is on the hot path, and it is only cheap while the list is
    /// short. The subscribed set bounds it in practice; the cap is what keeps
    /// that true if a venue starts naming symbols nobody asked for.
    #[tokio::test(start_paused = true)]
    async fn the_hot_path_scan_stays_bounded() {
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &[]);

        for i in 0..(MAX_TRACKED_SYMBOLS + 50) {
            gauge.record_write(Source::Venue, &format!("SYM{i}"));
        }

        let (record, _) = gauge.sample();
        assert_eq!(ages(&record).len(), MAX_TRACKED_SYMBOLS);
        assert_eq!(
            record["overflowed"], true,
            "the cap was hit without the record saying so: {record}"
        );
    }

    /// The other way past the cap is a subscription list longer than it. Those
    /// symbols were explicitly asked for, so dropping them quietly would leave
    /// the record reporting on a subset while looking complete.
    #[tokio::test(start_paused = true)]
    async fn a_subscription_list_past_the_cap_says_so() {
        let asked_for: Vec<String> = (0..(MAX_TRACKED_SYMBOLS + 1))
            .map(|i| format!("SYM{i}"))
            .collect();
        let mut gauge = LivenessGauge::new(THRESHOLD_S, &asked_for);

        let (record, _) = gauge.sample();

        assert_eq!(ages(&record).len(), MAX_TRACKED_SYMBOLS);
        assert_eq!(
            record["overflowed"], true,
            "symbols the operator asked for were dropped in silence: {record}"
        );
    }

    /// A venue with a periodic per-symbol feed can be held to something much
    /// tighter than one without, and the difference is what makes the number
    /// worth deriving rather than picking. Pinned to the venue names `main`
    /// dispatches on, including the alias.
    #[test]
    fn each_venue_gets_a_threshold_from_its_slowest_per_symbol_feed() {
        // ~1/s per coin, always on: activeAssetCtx / markPrice@1s.
        assert_eq!(default_threshold_s("hyperliquid"), 60);
        assert_eq!(default_threshold_s("binancefuturescm"), 60);

        // Order flow only, where a quiet symbol is legitimately silent.
        for venue in [
            "binancefutures",
            "binancefuturesum",
            "binance",
            "binancespot",
            "bybit",
            "something-added-later",
        ] {
            assert_eq!(
                default_threshold_s(venue),
                300,
                "{venue} was given a threshold tighter than its slowest feed"
            );
        }
    }
}
